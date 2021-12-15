/* Copyright 2020 Twilio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twilio.kudu.sql.mutation;

import com.twilio.kudu.sql.metadata.CubeTableInfo;
import org.apache.calcite.avatica.util.DateTimeUtils;
import com.twilio.kudu.sql.CalciteKuduTable;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.kudu.Schema;
import org.apache.kudu.util.Pair;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Used to calculate aggregated values and upsert rows to the kudu cube tables
 */
public class CubeMaintainer {

  private final Map<Integer, Integer> pkColIndexMap = new HashMap<>();
  private final Map<Integer, Integer> nonPkColIndexMap = new HashMap<>();
  private final int timestampColIndex;
  private final boolean timestampOrderedDesc;
  private final CubeTableInfo.EventTimeAggregationType eventTimeAggregationType;
  private int countRecordsColIndex;

  public CubeMaintainer(CalciteKuduTable cubeCalciteKuduTable, CalciteKuduTable factCalciteKuduTable) {
    Schema cubeSchema = cubeCalciteKuduTable.getKuduTable().getSchema();
    Schema factSchema = factCalciteKuduTable.getKuduTable().getSchema();
    timestampColIndex = cubeCalciteKuduTable.getTimestampColumnIndex();
    timestampOrderedDesc = cubeCalciteKuduTable.isColumnOrderedDesc(timestampColIndex);
    eventTimeAggregationType = cubeCalciteKuduTable.getEventTimeAggregationType();

    // build map from cube pk column index to fact column index
    for (int cubeColIndex = 0; cubeColIndex < cubeSchema.getPrimaryKeyColumnCount(); ++cubeColIndex) {
      String cubeColumnName = cubeSchema.getColumnByIndex(cubeColIndex).getName();
      pkColIndexMap.put(cubeColIndex, factSchema.getColumnIndex(cubeColumnName));
    }

    // build map from cube non-pk column index to fact column index
    for (int cubeColIndex = cubeSchema.getPrimaryKeyColumnCount(); cubeColIndex < cubeSchema
        .getColumnCount(); ++cubeColIndex) {
      // non pk columns are measure of the format {measure}_{column_name} for eg
      // sum_amount
      // count_records is handled as a special case
      String cubeColumnName = cubeSchema.getColumnByIndex(cubeColIndex).getName();
      if (cubeColumnName.equals("count_records")) {
        countRecordsColIndex = cubeColIndex;
        nonPkColIndexMap.put(cubeColIndex, -1);
      } else {
        String factColumnName = cubeColumnName.substring(cubeColumnName.indexOf("_") + 1);
        nonPkColIndexMap.put(cubeColIndex, factSchema.getColumnIndex(factColumnName));
      }
    }
  }

  /**
   * TODO support month and year
   * 
   * @return the mod value used to truncate the timestamp
   */
  public long getFloorMod() {
    switch (eventTimeAggregationType) {
    case second:
      return DateTimeUtils.MILLIS_PER_SECOND;
    case hour:
      return DateTimeUtils.MILLIS_PER_HOUR;
    case day:
      return DateTimeUtils.MILLIS_PER_DAY;
    case minute:
      return DateTimeUtils.MILLIS_PER_MINUTE;
    default:
      throw new UnsupportedOperationException("Cube is not supported for aggregation " + eventTimeAggregationType);
    }
  }

  /**
   * Remaps the fact table columns to the cube table pk columns (dimensions) and
   * the cube table non pk columns (measures) Also truncates the event time column
   * to the cube rollup time
   * 
   * @param colIndexToValueMap map from fact table column index to column value
   * @return pair of array of pk column values and array of non-pk column values
   */
  public Pair<Object[], Object[]> generateCubeDelta(Map<Integer, Object> colIndexToValueMap) {
    Object[] pkColumnValues = new Object[pkColIndexMap.size()];
    Object[] nonPkColumnValues = new Object[nonPkColIndexMap.size()];

    // set column values for dimensions
    int index = 0;
    for (Map.Entry<Integer, Integer> entry : pkColIndexMap.entrySet()) {
      int cubeColIndex = entry.getKey();
      int factColIndex = entry.getValue();
      Object columnValue = colIndexToValueMap.get(factColIndex);
      // if this is the timestamp column apply any truncation that is required to do
      // the rollup
      if (cubeColIndex == timestampColIndex) {
        if (timestampOrderedDesc) {
          Long timestamp = CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS - (Long) columnValue / 1000;
          Long truncatedTimestamp = SqlFunctions.floor(timestamp, getFloorMod());
          pkColumnValues[index++] = CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS - truncatedTimestamp * 1000;
        } else {
          Long timestamp = (Long) columnValue / 1000;
          Long truncatedTimestamp = SqlFunctions.floor(timestamp, getFloorMod());
          pkColumnValues[index++] = truncatedTimestamp * 1000;
        }
      } else {
        pkColumnValues[index++] = columnValue;
      }
    }

    // set column values for measures
    index = 0;
    for (Map.Entry<Integer, Integer> entry : nonPkColIndexMap.entrySet()) {
      int cubeColIndex = entry.getKey();
      int factColIndex = entry.getValue();
      if (cubeColIndex == countRecordsColIndex) {
        // add 1 for the count_records columns
        nonPkColumnValues[index++] = 1l;
      } else {
        nonPkColumnValues[index++] = colIndexToValueMap.get(factColIndex);
      }
    }
    return new Pair<>(pkColumnValues, nonPkColumnValues);
  }

  public Iterator<Integer> getNonPKColumnIndexes() {
    return nonPkColIndexMap.keySet().iterator();
  }

  public Iterator<Integer> gePKColumnIndexes() {
    return pkColIndexMap.keySet().iterator();
  }

}
