package com.twilio.raas.sql.mutation;

import com.twilio.raas.sql.CalciteModifiableKuduTable;
import org.apache.calcite.util.Pair;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CubeMutationState extends MutationState {

  private static final Logger logger = LoggerFactory.getLogger(CubeMutationState.class);

  // map from dimensions (pk columns of cube table) to measures (non pk columns)
  // the inner map is from column index to column value
  // the event time column value is truncated to the cube time rollup
  private final Map<Map<Integer, Object>, Map<Integer, Object>> aggregatedValues = new HashMap<>();

  // set containing the cube pk row values that represent the current fact rows in the batch that
  // were aggregated
  // used so that we limit the data being upserted to each cube that is maintained
  private final Set<Map<Integer, Object>> currentBatchAggregations = new HashSet<>();

  public CubeMutationState(CalciteModifiableKuduTable calciteModifiableKuduTable) {
    super(calciteModifiableKuduTable);
  }

  private Object increment(int columnIndex, Object currentValue, Object deltaValue) {
    Type columnType =
      calciteModifiableKuduTable.getKuduTable().getSchema().getColumnByIndex(columnIndex).getType();
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
        throw new UnsupportedOperationException("Aggregation over type " + columnType + " is not " +
          "supported");
    }
  }

  /**
   * Updates the aggregated values for this cube table that is used to generate an upsert when
   * commit is called
   */
  @Override
  protected void updateMutationState(Map<Integer, Object> colIndexToValueMap) {
    Pair<Map<Integer, Object>, Map<Integer, Object>> cubeDeltaRow =
      calciteModifiableKuduTable.getCubeMaintainer().generateCubeDelta(colIndexToValueMap);

    if (aggregatedValues.containsKey(cubeDeltaRow.left)) {
      Map<Integer, Object> currentAggregatedColValues = aggregatedValues.get(cubeDeltaRow.left);
      for (Map.Entry<Integer, Object> entry : currentAggregatedColValues.entrySet()) {
        Integer colIndex = entry.getKey();
        Object currentValue = entry.getValue();
        Object deltaValue = cubeDeltaRow.right.get(colIndex);
        currentAggregatedColValues.put(colIndex, increment(colIndex, currentValue, deltaValue));
      }
    }
    else {
      aggregatedValues.put(cubeDeltaRow.left, cubeDeltaRow.right);
    }
    currentBatchAggregations.add(cubeDeltaRow.left);
  }

  @Override
  public void flush() {
    if (currentBatchAggregations.isEmpty()) {
      return;
    }
    long startTime = System.currentTimeMillis();
    int mutationCount = 0;
    try {
      for (Map<Integer, Object> cubePK : currentBatchAggregations) {
        final Upsert upsert = kuduTable.newUpsert();
        final PartialRow partialRow = upsert.getRow();
        // set the pk values
        for (Map.Entry<Integer, Object> pkEntry : cubePK.entrySet()) {
          partialRow.addObject(pkEntry.getKey(), pkEntry.getValue());
        }
        // set the non pk values
        for (Map.Entry<Integer, Object> nonPkEntry : aggregatedValues.get(cubePK).entrySet()) {
          partialRow.addObject(nonPkEntry.getKey(), nonPkEntry.getValue());
        }
        session.apply(upsert);
        // TODO make this configurable
        if (mutationCount++ % 1000 == 0) {
          session.flush();
        }
      }
      // send the last batch
      session.flush();
    }
    catch (KuduException e) {
      throw new RuntimeException(e);
    }
    finally {
      // clear the list of cube pk values
      currentBatchAggregations.clear();
    }
    logger.info("Map size " + aggregatedValues.size() +" rows. Flushed " + mutationCount +" cube " +
      "rows in " + (System.currentTimeMillis() - startTime) + " ms.");
  }

}
