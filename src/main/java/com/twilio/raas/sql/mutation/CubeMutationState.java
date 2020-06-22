package com.twilio.raas.sql.mutation;

import com.twilio.raas.sql.CalciteKuduTable;
import com.twilio.raas.sql.CalciteModifiableKuduTable;
import org.apache.calcite.util.Pair;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class CubeMutationState extends MutationState {

  // map from dimensions (pk columns of cube table) to measures (non pk columns)
  // the inner map is from column index to column value
  // the event time column value is truncated to the cube time rollup
  private final Map<Map<Integer, Object>, Map<Integer, Object>> aggregatedValues = new HashMap<>();

  public CubeMutationState(CalciteModifiableKuduTable calciteKuduTable) {
    super(calciteKuduTable);
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
  }

  @Override
  public void flush() {
    int mutationCount = 0;
    for (Map.Entry<Map<Integer, Object>, Map<Integer, Object>> entry : aggregatedValues.entrySet()) {
      final Upsert upsert = kuduTable.newUpsert();
      final PartialRow partialRow = upsert.getRow();
      // set the pk values
      for (Map.Entry<Integer, Object> pkEntry : entry.getKey().entrySet()) {
        partialRow.addObject(pkEntry.getKey(), pkEntry.getValue());
      }
      // set the non pk values
      for (Map.Entry<Integer, Object> nonPkEntry : entry.getValue().entrySet()) {
        partialRow.addObject(nonPkEntry.getKey(), nonPkEntry.getValue());
      }
      try {
        session.apply(upsert);
        // TODO make this configurable
        if (mutationCount % 1000 == 0) {
          session.flush();
        }
      } catch (KuduException e) {
        throw new RuntimeException(e);
      }
    }
    // send the last batch
    try {
      session.flush();
    } catch (KuduException e) {
      throw new RuntimeException(e);
    }
  }

}
