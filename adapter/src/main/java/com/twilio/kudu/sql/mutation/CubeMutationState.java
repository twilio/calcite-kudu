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

import com.twilio.kudu.sql.CalciteModifiableKuduTable;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.util.ByteVec;
import org.apache.kudu.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CubeMutationState extends MutationState {

  private static final Logger logger = LoggerFactory.getLogger(CubeMutationState.class);

  // map from group by columns (pk columns of cube table encoded as a byte[]) to
  // the aggregated column values
  // the event time pk column value is truncated to the cube time rollup
  private final Map<ByteVec, Object[]> aggregatedValues = new HashMap<>();

  // list of pair of pk column values and non-pk columnn values in the current
  // batch to be
  // written to the cube table
  private final List<Pair<Object[], Object[]>> currentBatchAggregations = new ArrayList<>();

  public CubeMutationState(CalciteModifiableKuduTable calciteModifiableKuduTable) {
    super(calciteModifiableKuduTable);
  }

  private Object increment(int columnIndex, Object currentValue, Object deltaValue) {
    Type columnType = calciteModifiableKuduTable.getKuduTable().getSchema().getColumnByIndex(columnIndex).getType();
    switch (columnType) {
    case INT8:
      return (byte) ((byte) currentValue + (byte) deltaValue);
    case INT16:
      return (short) ((short) currentValue + (short) deltaValue);
    case INT32:
      return (int) currentValue + (int) deltaValue;
    case INT64:
      return (long) currentValue + (long) deltaValue;
    case FLOAT:
      return (float) currentValue + (float) deltaValue;
    case DOUBLE:
      return (double) currentValue + (double) deltaValue;
    case DECIMAL:
      return ((BigDecimal) currentValue).add((BigDecimal) deltaValue);
    default:
      throw new UnsupportedOperationException("Aggregation over type " + columnType + " is not " + "supported");
    }
  }

  /**
   * Updates the aggregated values for this cube table that is used to generate an
   * upsert when commit is called
   */
  @Override
  protected void updateMutationState(Map<Integer, Object> colIndexToValueMap) {
    Pair<Object[], Object[]> cubeDeltaRow = calciteModifiableKuduTable.getCubeMaintainer()
        .generateCubeDelta(colIndexToValueMap);

    // If cube aggregations are disabled then just use the write the cube delta
    // value to the cube
    // table. This is used to speed up the DataLoader when running performance
    // tests.
    if (calciteModifiableKuduTable.isDisableCubeAggregations()) {
      currentBatchAggregations.add(new Pair<>(cubeDeltaRow.getFirst(), cubeDeltaRow.getSecond()));
    } else {
      final Upsert upsert = kuduTable.newUpsert();
      final PartialRow row = upsert.getRow();
      // set the pk values in partialRow
      Iterator<Integer> pkColumnIndexIterator = calciteModifiableKuduTable.getCubeMaintainer().gePKColumnIndexes();
      for (Object pKColValue : cubeDeltaRow.getFirst()) {
        row.addObject(pkColumnIndexIterator.next(), pKColValue);
      }
      ByteVec rowKey = ByteVec.wrap(row.encodePrimaryKey());

      Iterator<Integer> nonPKColIndexIterator = calciteModifiableKuduTable.getCubeMaintainer().getNonPKColumnIndexes();
      if (aggregatedValues.containsKey(rowKey)) {
        Object[] currentAggregatedColValues = aggregatedValues.get(rowKey);
        for (int i = 0; i < currentAggregatedColValues.length; ++i) {
          Object currentValue = currentAggregatedColValues[i];
          Integer colIndex = nonPKColIndexIterator.next();
          Object deltaValue = cubeDeltaRow.getSecond()[i];
          currentAggregatedColValues[i] = increment(colIndex, currentValue, deltaValue);
        }
        currentBatchAggregations.add(new Pair<>(cubeDeltaRow.getFirst(), currentAggregatedColValues));
      } else {
        aggregatedValues.put(rowKey, cubeDeltaRow.getSecond());
        currentBatchAggregations.add(new Pair<>(cubeDeltaRow.getFirst(), cubeDeltaRow.getSecond()));
      }
    }
  }

  @Override
  public void flush() {
    if (currentBatchAggregations.isEmpty()) {
      return;
    }
    long startTime = System.currentTimeMillis();
    int mutationCount = 0;
    try {
      for (Pair<Object[], Object[]> cubeValuePair : currentBatchAggregations) {
        final Upsert upsert = kuduTable.newUpsert();
        final PartialRow partialRow = upsert.getRow();
        int index = 0;
        // set the pk values
        for (Object pkColVal : cubeValuePair.getFirst()) {
          partialRow.addObject(index++, pkColVal);
        }
        // set the non pk values
        for (Object nonPKColValue : cubeValuePair.getSecond()) {
          partialRow.addObject(index++, nonPKColValue);
        }
        session.apply(upsert);
        // TODO make this configurable
        if (mutationCount++ % 1000 == 0) {
          session.flush();
        }
      }
      // send the last batch
      session.flush();
    } catch (KuduException e) {
      throw new RuntimeException(e);
    } finally {
      // clear the list of cube pk values
      currentBatchAggregations.clear();
    }
    logger.info("Cube table {} map size {} rows. Flushed mutationCount cube rows in {} ms.", kuduTable.getName(),
        aggregatedValues.size(), (System.currentTimeMillis() - startTime));
  }

  public void clear() {
    aggregatedValues.clear();
    currentBatchAggregations.clear();
  }

}
