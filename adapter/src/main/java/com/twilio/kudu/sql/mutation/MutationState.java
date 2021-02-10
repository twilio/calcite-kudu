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
import org.apache.calcite.avatica.util.ByteString;
import com.twilio.kudu.sql.CalciteKuduTable;
import org.apache.calcite.rex.RexLiteral;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MutationState {

  private static final Logger logger = LoggerFactory.getLogger(MutationState.class);

  protected final CalciteModifiableKuduTable calciteModifiableKuduTable;
  protected final KuduTable kuduTable;
  protected final KuduSession session;

  private int numFactRowsInBatch = 0;

  private List<CubeMutationState> cubeMutationStateList = new ArrayList<>();

  public MutationState(final CalciteModifiableKuduTable calciteModifiableKuduTable) {
    this.calciteModifiableKuduTable = calciteModifiableKuduTable;
    this.kuduTable = calciteModifiableKuduTable.getKuduTable();
    this.session = calciteModifiableKuduTable.getClient().syncClient().newSession();
    session.setFlushMode(AsyncKuduSession.FlushMode.MANUAL_FLUSH);
    for (CalciteKuduTable cubeTable : this.calciteModifiableKuduTable.getCubeTables()) {
      cubeMutationStateList.add(new CubeMutationState((CalciteModifiableKuduTable) cubeTable));
    }
  }

  /**
   * Returns the Java type that a literal should be converted to
   */
  private Class getDataType(int columnIndex) {
    ColumnSchema col = kuduTable.getSchema().getColumnByIndex(columnIndex);
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
   * Returns the column value to be stored in kudu. For primary columns that are
   * stored in descending order, we invert the column value so that they natural
   * ordering is inverted To invert a column value : COL_MAX_VAL - value +
   * COL_MIN_VAL which can be simplified to : -1 - value This translates the max
   * column value to the min column value, min column value to the max column
   * value and all the values in between are similarly translated.
   *
   * For descending ordered timestamps, we just subtract from
   * EPOCH_DAY_FOR_REVERSE_SORT since we don't allow negative timestamps
   */
  private Object getColumnValue(int colIndex, Object value) {
    if (value == null) {
      return value;
    }
    ColumnSchema col = kuduTable.getSchema().getColumnByIndex(colIndex);
    if (calciteModifiableKuduTable.isColumnOrderedDesc(colIndex)) {
      switch (col.getType()) {
      case INT8:
        return (byte) (-1 - (byte) value);
      case INT16:
        return (short) (-1 - (short) value);
      case INT32:
        return -1 - (int) value;
      case INT64:
        return -1l - (long) value;
      case UNIXTIME_MICROS:
        long timestamp = (long) value;
        if (timestamp < 0) {
          throw new IllegalArgumentException(
              "Storing negative timstamp values for a column " + "ordered descending is not supported");
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
   * Mutate Kudu table using {@link RexLiteral}. Called while using a regular
   * Statement
   *
   * @param columnIndexes the kudu indexes to mutate
   * @param tuples        a collection of rows to insert
   * @return number of rows inserted
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
   * Mutate Kudu table using {@link Object}.
   *
   * @param columnIndexes the kudu indexes to mutate
   * @param values        a collection of rows to insert
   * @return number of rows inserted
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
   * Creates a mutation for the row being inserted and adds it to the kudu
   * session. Also calls {@code updateMutationState()} for all cube tables (if any
   * exist).
   *
   * @param colIndexToValueMap Kudu index to new value mapping
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
    for (MutationState cubeMutationState : cubeMutationStateList) {
      cubeMutationState.updateMutationState(colIndexToValueMap);
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
      logger
          .info("Flushed " + numFactRowsInBatch + " fact rows in " + (System.currentTimeMillis() - startTime) + " ms");
      numFactRowsInBatch = 0;
      // flush aggregated values for each cube table
      for (MutationState cubeMutationState : cubeMutationStateList) {
        cubeMutationState.flush();
      }

    } catch (KuduException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Clears the cube mutation state (and thus frees up memory)
   */
  public void clear() {
    for (MutationState cubeMutationState : cubeMutationStateList) {
      cubeMutationState.clear();
    }
    numFactRowsInBatch = 0;
  }

}
