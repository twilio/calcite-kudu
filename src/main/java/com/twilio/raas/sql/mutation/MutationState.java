package com.twilio.raas.sql.mutation;

import com.twilio.raas.sql.CalciteKuduTable;
import com.twilio.raas.sql.CalciteModifiableKuduTable;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rex.RexLiteral;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MutationState {

  private static final Logger logger = LoggerFactory.getLogger(MutationState.class);

  protected final KuduClient kuduClient;
  protected final CalciteModifiableKuduTable calciteModifiableKuduTable;
  protected final KuduTable kuduTable;
  protected final KuduSession session;

  private int numFactRowsInBatch = 0;

  public MutationState(final CalciteModifiableKuduTable calciteKuduTable) {
    this.calciteModifiableKuduTable = calciteKuduTable;
    this.kuduClient = calciteKuduTable.getClient().syncClient();
    this.kuduTable = calciteKuduTable.getKuduTable();
    this.session = kuduClient.newSession();
    session.setFlushMode(AsyncKuduSession.FlushMode.MANUAL_FLUSH);
  }

  /**
   * Returns the Java type that a literal should be converted to
   */
  private Class getDataType(int columnIndex) {
    ColumnSchema col = calciteModifiableKuduTable.getKuduTable().getSchema().getColumnByIndex(columnIndex);
    switch (col.getType()) {
      case BOOL:
        return Boolean.class;
      case INT8:
        return Byte.class;
      case INT16:
        return Short.class;
      case INT32:
        return Integer.class;
      case INT64:
      case UNIXTIME_MICROS:
        return Long.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case STRING:
        return String.class;
      case BINARY:
        return byte[].class;
      case DECIMAL:
        return BigDecimal.class;
      default:
        throw new IllegalArgumentException("Unsupported column type: " + col.getType());
    }
  }

  /**
   * Returns the column value to be stored in kudu. For primary columns that are stored in
   * descending order, we invert the column value so that they natural ordering is inverted
   * To invert a column value : COL_MAX_VAL - value + COL_MIN_VAL
   * This translates the max column value to the min column value, min column value to the max
   * column value and all the values in between are similarly translated.
   *
   * For descending ordered timestamps, we just subtract from  EPOCH_DAY_FOR_REVERSE_SORT
   * since we don't allow negative timestamps
   */
  private Object getColumnValue(int colIndex, Object value) {
    if (value == null) {
      return value;
    }
    ColumnSchema col = kuduTable.getSchema().getColumnByIndex(colIndex);
    if (calciteModifiableKuduTable.isColumnOrderedDesc(colIndex)) {
      switch (col.getType()) {
        case INT8:
          return (byte) (Byte.MAX_VALUE - (byte) value + Byte.MIN_VALUE);
        case INT16:
          return (short) (Short.MAX_VALUE - (short) value + Short.MIN_VALUE);
        case INT32:
          return (Integer.MAX_VALUE - (int) value + Integer.MIN_VALUE);
        case INT64:
          return (Long.MAX_VALUE - (long) value + Long.MIN_VALUE);
        case UNIXTIME_MICROS:
          long timestamp =  (long) value;
          if ( timestamp <0 ) {
            throw new IllegalArgumentException("Storing negative timstamp values for a column " +
              "ordered descending is not supported");
          }
          return (CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS - timestamp) * 1000;
        case BINARY:
          return ((ByteString) value).getBytes();
        default:
          return value;
      }
    } else {
      switch (col.getType()) {
        case UNIXTIME_MICROS:
          return ((Long) value) * 1000;
        case BINARY:
          return ((ByteString) value).getBytes();
        default:
          return value;
      }
    }
  }

  /**
   * Called while using a regular Statement
   */
  public int mutateTuples(final List<Integer> columnIndexes, final List<List<RexLiteral>> tuples) {
    for (List<RexLiteral> tuple : tuples) {
      Map<Integer, Object> colIndexToValueMap = new HashMap<>();
      for (int i = 0; i < columnIndexes.size(); ++i) {
        int columnIndex = columnIndexes.get(i);
        Class dataType = getDataType(columnIndexes.get(i));
        Object value = getColumnValue(columnIndex, tuple.get(i).getValueAs(dataType));
        colIndexToValueMap.put(columnIndex, value);
      }
      updateMutationState(colIndexToValueMap);
    }
    return tuples.size();
  }

  /**
   * Called while using a PreparedStatement
   */
  public int mutateRow(final List<Integer> columnIndexes, final List<Object> values) {
    Map<Integer, Object> colIndexToValueMap = new HashMap<>();
    for (int i = 0; i < columnIndexes.size(); ++i) {
      int columnIndex = columnIndexes.get(i);
      colIndexToValueMap.put(columnIndex, getColumnValue(columnIndex, values.get(i)));
    }
    updateMutationState(colIndexToValueMap);
    return 1;
  }

  /**
   * Creates a mutation for the row being inserted and adds it to the kudu session.
   * Also calls updateMutationState() for all cube tables (if any exist).
   */
  protected void updateMutationState(Map<Integer, Object> colIndexToValueMap) {
    final Insert insert = kuduTable.newInsert();
    final PartialRow partialRow = insert.getRow();
    for (Map.Entry<Integer, Object> entry : colIndexToValueMap.entrySet()) {
      partialRow.addObject(entry.getKey(), entry.getValue());
    }
    try {
      session.apply(insert);
      ++numFactRowsInBatch;
    } catch (KuduException e) {
      throw new RuntimeException(e);
    }

    // update aggregated values for each cube table
    for (CalciteKuduTable cubeTable : calciteModifiableKuduTable.getCubeTables()) {
      ((CalciteModifiableKuduTable)cubeTable).getMutationState().updateMutationState(colIndexToValueMap);
    }
  }

  public void flush() {
    if (numFactRowsInBatch == 0) {
      return;
    }
    try {
      long startTime = System.currentTimeMillis();
      // flush fact table rows
      List<OperationResponse> responseList = session.flush();
      for (OperationResponse op : responseList) {
        if (op != null && op.hasRowError() && op.getRowError().getErrorStatus().isAlreadyPresent()) {
          throw new RuntimeException("Row already exists " + op.getRowError().getOperation());
        }
      }
      logger.info("Flushed " + numFactRowsInBatch +" fact rows in " + (System.currentTimeMillis() - startTime) + " ms");
      numFactRowsInBatch = 0;
      // flush aggregated values for each cube table
      for (CalciteKuduTable cubeTable : calciteModifiableKuduTable.getCubeTables()) {
          ((CalciteModifiableKuduTable) cubeTable).getMutationState().flush();
      }

    } catch (KuduException e) {
      throw new RuntimeException(e);
    }
  }

}
