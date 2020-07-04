package com.twilio.raas.dataloader;

import com.twilio.raas.dataloader.generator.ColumnValueGenerator;
import com.twilio.raas.sql.CalciteKuduTable;
import com.twilio.raas.sql.JDBCUtil;
import com.twilio.raas.sql.mutation.MutationState;
import com.twilio.raas.sql.schema.KuduSchemaFactory;
import org.apache.kudu.ColumnSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class DataLoader {

  private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);

  private final Scenario scenario;
  private final CalciteKuduTable calciteKuduTable;
  private String url;

  public DataLoader(final String url, final Scenario scenario) throws SQLException {
    DriverManager.getConnection(url);
    this.scenario = scenario;
    this.calciteKuduTable =
      KuduSchemaFactory.INSTANCE.getTable(scenario.getTableName())
        .orElseThrow(() -> new RuntimeException("Table not found " + scenario.getTableName()));
    this.url = url;
  }

  private String buildSql() {
    StringBuilder builder = new StringBuilder();
    builder.append("UPSERT INTO \"");
    builder.append(calciteKuduTable.getKuduTable().getName());
    builder.append("\" (");
    boolean isFirst = true;
    for (ColumnSchema columnSchema : calciteKuduTable.getKuduTable().getSchema().getColumns()) {
      if (isFirst) {
        isFirst = false;
      }
      else {
        builder.append(",");
      }
      builder.append(columnSchema.getName());
    }
    builder.append(") VALUES (");
    for (int i = 0; i < calciteKuduTable.getKuduTable().getSchema().getColumnCount(); i++) {
      if (i < calciteKuduTable.getKuduTable().getSchema().getColumnCount() - 1) {
        builder.append("?,");
      } else {
        builder.append("?)");
      }
    }
    return builder.toString();
  }

  private ColumnValueGenerator<?> getColumnValueGenerator(String columnName) {
    if (!scenario.getColumnNameToValueGenerator().containsKey(columnName)) {
      throw new IllegalStateException("No generator found for column " + columnName);
    }
    return scenario.getColumnNameToValueGenerator().get(columnName);
  }

  private void bindValues(PreparedStatement statement) throws SQLException {
    int count = 1;
    for (ColumnSchema columnSchema : calciteKuduTable.getKuduTable().getSchema().getColumns()) {
      switch (columnSchema.getType()) {
        case INT8:
          Byte byteValue;
          Object value = getColumnValueGenerator(columnSchema.getName()).getColumnValue();
          if (value instanceof Byte) {
            byteValue = (Byte) value;
          }
          else {
            byteValue = ((Integer) value).byteValue();
          }
          if (byteValue == null) {
            statement.setNull(count, Types.TINYINT);
          } else {
            statement.setByte(count, byteValue);
          }
          break;
        case INT16:
          Short shortValue =
            ((ColumnValueGenerator<Short>) getColumnValueGenerator(columnSchema.getName())).getColumnValue();
          if (shortValue == null) {
            statement.setNull(count, Types.SMALLINT);
          } else {
            statement.setShort(count, shortValue);
          }
          break;
        case INT32:
          Integer intValue =
            ((ColumnValueGenerator<Integer>) getColumnValueGenerator(columnSchema.getName())).getColumnValue();
          if (intValue == null) {
            statement.setNull(count, Types.INTEGER);
          } else {
            statement.setInt(count, intValue);
          }
          break;
        case UNIXTIME_MICROS:
        case INT64:
          Long longValue =
            ((ColumnValueGenerator<Long>) getColumnValueGenerator(columnSchema.getName())).getColumnValue();
          if (longValue == null) {
            statement.setNull(count, Types.BIGINT);
          } else {
            statement.setLong(count, longValue);
          }
          break;
        case STRING:
          String stringValue =
            ((ColumnValueGenerator<String>) getColumnValueGenerator(columnSchema.getName())).getColumnValue();
          if (stringValue == null || stringValue.equals("")) {
            statement.setNull(count, Types.VARCHAR);
          } else {
            statement.setString(count, stringValue);
          }
          break;
        case BOOL:
          Boolean booleanValue =
            ((ColumnValueGenerator<Boolean>) getColumnValueGenerator(columnSchema.getName())).getColumnValue();
          if (booleanValue == null || booleanValue.equals("")) {
            statement.setNull(count, Types.VARCHAR);
          } else {
            statement.setBoolean(count, booleanValue);
          }
          break;
        case FLOAT:
          Float floatValue =
            ((ColumnValueGenerator<Float>) getColumnValueGenerator(columnSchema.getName())).getColumnValue();
          if (floatValue == null) {
            statement.setNull(count, Types.FLOAT);
          } else {
            statement.setFloat(count, floatValue);
          }
          break;
        case DOUBLE:
          Double doubleVal =
            ((ColumnValueGenerator<Double>) getColumnValueGenerator(columnSchema.getName())).getColumnValue();
          if (doubleVal == null) {
            statement.setNull(count, Types.DOUBLE);
          } else {
            statement.setDouble(count, doubleVal);
          }
          break;
        case DECIMAL:
          BigDecimal decimalVal =
            ((ColumnValueGenerator<BigDecimal>)  getColumnValueGenerator(columnSchema.getName())).getColumnValue();
          if (decimalVal == null) {
            statement.setNull(count, Types.DOUBLE);
          } else {
            statement.setBigDecimal(count, decimalVal);
          }
          break;
        default:
          throw new UnsupportedOperationException("Unable to handle data type " + columnSchema.getType());
      }
      count++;
    }
  }

  public void loadData() throws SQLException {
    try (Connection conn = DriverManager.getConnection(url)) {
      // Create prepared statement that can be reused
      String sql = buildSql();
      PreparedStatement stmt = conn.prepareStatement(sql);

      // populate table with data
      long startTime = System.currentTimeMillis();
      for (int i = 1; i <= scenario.getNumRows(); ++i) {
        bindValues(stmt);
        stmt.execute();
        if (i % 1000 == 0) {
          conn.commit();
          logger.info("Total number of rows committed " + i + " time taken for " +
            "current batch " + (System.currentTimeMillis() - startTime));
          startTime = System.currentTimeMillis();
        }
      }
      conn.commit();
    }
  }

}

