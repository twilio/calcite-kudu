package com.twilio.raas.sql;

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rex.RexLiteral;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Status;

import java.math.BigDecimal;
import java.util.List;

public class KuduMutation {

  private final KuduClient kuduClient;
  private final CalciteKuduTable calciteKuduTable;
  private final KuduTable kuduTable;
  private final List<String> columnNames;

  public KuduMutation(final CalciteKuduTable kuduTable,
                      final List<String> columnNames) {
    this.calciteKuduTable = kuduTable;
    this.kuduClient = calciteKuduTable.getClient().syncClient();
    this.kuduTable = calciteKuduTable.getKuduTable();
    this.columnNames = columnNames;
  }

  /**
   * Returns the Java type that a literal should be converted to
   */
  private Class getDataType(String columnName) {
    ColumnSchema col = calciteKuduTable.getKuduTable().getSchema().getColumn(columnName);
    switch (col.getType()) {
      case BOOL: return Boolean.class;
      case INT8: return Byte.class;
      case INT16: return Short.class;
      case INT32: return Integer.class;
      case INT64:
      case UNIXTIME_MICROS:
        return Long.class;
      case FLOAT: return Float.class;
      case DOUBLE: return Double.class;
      case STRING: return String.class;
      case BINARY: return byte[].class;
      case DECIMAL: return BigDecimal.class;
      default:
        throw new IllegalArgumentException("Unsupported column type: " + col.getType());
    }
  }

  /**
   * Called while using a regular Statement
   */
  public int mutateTuples(final List<List<RexLiteral>> tuples) {
    final KuduSession session = kuduClient.newSession();
    for (List<RexLiteral> tuple : tuples) {
      final Insert insert = kuduTable.newInsert();
      for (int i=0; i< columnNames.size(); ++i) {
        final PartialRow partialRow = insert.getRow();
        String columnName = columnNames.get(i);
        Class dataType = getDataType(columnName);
        Object value = getValue(columnName, tuple.get(i).getValueAs(dataType));
        partialRow.addObject(columnName, value);
      }
      try {
        OperationResponse op = session.apply(insert);
        if (op.hasRowError() && op.getRowError().getErrorStatus().isAlreadyPresent()) {
          throw new RuntimeException("Row aleady exists " + tuple);
        }
      } catch (KuduException e) {
        throw new RuntimeException(e);
      }
    }
    return tuples.size();
  }

  private Object getValue(String columnName, Object value) {
    if (value == null) {
      return value;
    }
    ColumnSchema col = kuduTable.getSchema().getColumn(columnName);
    if (calciteKuduTable.isColumnSortedDesc(columnName)) {
      switch (col.getType()) {
        case INT8:
          return (byte) (Byte.MAX_VALUE - (Byte) value);
        case INT16:
          return (short) (Short.MAX_VALUE - (Short) value);
        case INT32:
          return (Integer.MAX_VALUE - (Integer) value);
        case INT64:
          return (Long.MAX_VALUE - (Long) value);
        case UNIXTIME_MICROS:
          return (CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS - (long) value) * 1000;
        case BINARY:
          return ((ByteString) value).getBytes();
        default:
          return value;
      }
    }
    else {
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
   * Called while using a PreparedStatement
   */
  public int mutateRow(final List<Object> values) {
    final KuduSession session = kuduClient.newSession();
    final Insert insert = kuduTable.newInsert();
    for (int i=0; i< columnNames.size(); ++i) {
      final PartialRow partialRow = insert.getRow();
      String columnName = columnNames.get(i);
      partialRow.addObject(columnName, getValue(columnName, values.get(i)));
    }
    try {
      session.apply(insert);
    } catch (KuduException e) {
      throw new RuntimeException(e);
    }
    return 1;
  }

}
