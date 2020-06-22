package com.twilio.raas.sql.mutation;

import com.twilio.kudu.metadata.CubeTableInfo;
import com.twilio.raas.sql.CalciteKuduTable;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.util.Pair;
import org.apache.kudu.Schema;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

  public CubeMaintainer(CalciteKuduTable cubeCalciteKuduTable,
                        CalciteKuduTable factCalciteKuduTable) {
    Schema cubeSchema = cubeCalciteKuduTable.getKuduTable().getSchema();
    Schema factSchema = factCalciteKuduTable.getKuduTable().getSchema();
    timestampColIndex = cubeCalciteKuduTable.getTimestampColumnIndex();
    timestampOrderedDesc = cubeCalciteKuduTable.isColumnOrderedDesc(timestampColIndex);
    eventTimeAggregationType = cubeCalciteKuduTable.getEventTimeAggregationType();

    // build map from cube pk column index to fact column index
    for (int cubeColIndex=0; cubeColIndex<cubeSchema.getPrimaryKeyColumnCount(); ++cubeColIndex) {
      String cubeColumnName = cubeSchema.getColumnByIndex(cubeColIndex).getName();
      pkColIndexMap.put(cubeColIndex, factSchema.getColumnIndex(cubeColumnName));
    }

    // build map from cube non-pk column index to fact column index
    for (int cubeColIndex = cubeSchema.getPrimaryKeyColumnCount(); cubeColIndex < cubeSchema.getColumnCount(); ++cubeColIndex) {
      // non pk columns are measure of the format {measure}_{column_name} for eg sum_amount
      // count_records is handled as a special case
      String cubeColumnName = cubeSchema.getColumnByIndex(cubeColIndex).getName();
      if (cubeColumnName.equals("count_records")) {
        countRecordsColIndex = cubeColIndex;
      }
      else {
        String factColumnName = cubeColumnName.substring(cubeColumnName.indexOf("_") + 1);
        nonPkColIndexMap.put(cubeColIndex, factSchema.getColumnIndex(factColumnName));
      }
    }
  }

  /**
   * TODO support month and year
   * @return the mod value used to truncate the timestamp
   */
  private long getFloorMod() {
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
   * Remaps the fact table columns to the cube table pk columns (dimensions) and the cube table
   * non pk columns (measures)
   * Also truncates the event time column to the cube rollup time
   * @param colIndexToValueMap  map from fact table column index to column value
   * @return  pair of ( map from cube table pk column index to column value, map from cube table
   * non-pk column index to column value)
   */
  public Pair<Map<Integer, Object>, Map<Integer, Object>> generateCubeDelta(Map<Integer, Object> colIndexToValueMap) {
    Map<Integer, Object> pkColumnValuesMap = new HashMap<>();
    Map<Integer, Object> nonPkColumnValuesMap = new HashMap<>();

    // set column values for dimensions
    for (Map.Entry<Integer, Integer> entry : pkColIndexMap.entrySet()) {
      int cubeColIndex = entry.getKey();
      int factColIndex = entry.getValue();
      Object columnValue = colIndexToValueMap.get(factColIndex);
      // if this is the timestamp column apply any truncation that is required to do the rollup
      if (cubeColIndex == timestampColIndex) {
        if (timestampOrderedDesc) {
          Long timestamp =
            CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS - (Long) columnValue/ 1000;
          Long truncatedTimestamp =
            SqlFunctions.floor(timestamp, getFloorMod());
          pkColumnValuesMap.put(cubeColIndex,
            CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS - truncatedTimestamp * 1000);
        }
        else {
          Long timestamp = (Long) columnValue/ 1000;
          Long truncatedTimestamp =
            DateTimeUtils.unixTimestampFloor(TimeUnitRange.DAY, timestamp);
          pkColumnValuesMap.put(cubeColIndex, truncatedTimestamp * 1000);
        }
      }
      else {
        pkColumnValuesMap.put(cubeColIndex, columnValue);
      }
    }

    // set column values for measures
    for (Map.Entry<Integer, Integer> entry : nonPkColIndexMap.entrySet()) {
      int cubeColIndex = entry.getKey();
      int factColIndex = entry.getValue();
      nonPkColumnValuesMap.put(cubeColIndex, colIndexToValueMap.get(factColIndex));
    }
    // add 1 for the count_records columns
    nonPkColumnValuesMap.put(countRecordsColIndex, 1l);

    return new Pair<>(pkColumnValuesMap, nonPkColumnValuesMap);
  }



}
