package com.twilio.raas.sql;

import org.apache.calcite.util.TimestampString;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KuduWriteIT {

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();
  public static final String BASE_TABLE_NAME = "SCHEMA.TABLE";
  private static final String CUSTOM_TEMPLATE = "jdbc:calcite:model=inline:{version: '1.0'," +
    "defaultSchema:'kudu',schemas:[{name: 'kudu',type:'custom',factory:'com.twilio.raas.sql" +
    ".KuduSchemaFactory',operand:{connect:'%s',kuduTableConfigs:[{tableName: '" + BASE_TABLE_NAME +
    "', descendingSortedFields:['UNIXTIME_MICROS_COL']}]}}]};caseSensitive=false;timeZone=UTC";


  public static KuduTable kuduTable;

  @BeforeClass
  public static void setup() throws Exception {
    ColumnTypeAttributes decimalTypeAttribute =
      new ColumnTypeAttributes.ColumnTypeAttributesBuilder().scale(6).precision(22).build();
    final List<ColumnSchema> columns = Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("STRING_COL", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("UNIXTIME_MICROS_COL", Type.UNIXTIME_MICROS).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("INT8_COL", Type.INT8).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("INT16_COL", Type.INT16).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("INT32_COL", Type.INT32).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("INT64_COL", Type.INT64).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("BINARY_COL", Type.BINARY).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("BOOL_COL", Type.BOOL).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("FLOAT_COL", Type.FLOAT).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("DOUBLE_COL", Type.DOUBLE).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("DECIMAL_COL", Type.DECIMAL).nullable(true)
        .typeAttributes(decimalTypeAttribute).build()
      );

    testHarness.getClient().createTable(BASE_TABLE_NAME, new Schema(columns),
      new org.apache.kudu.client.CreateTableOptions()
        .addHashPartitions(Arrays.asList("INT8_COL"), 2)
        .setRangePartitionColumns(Arrays.asList("UNIXTIME_MICROS_COL"))
        .setNumReplicas(1));

    kuduTable = testHarness.getClient().openTable(BASE_TABLE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testHarness.getClient().deleteTable(BASE_TABLE_NAME);
  }

  @Test
  public void testPreparedStatement() throws Exception {
    String url = String.format(CUSTOM_TEMPLATE,
      testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      String stringVal = "ACCOUNT1";

      // Create prepared statement that can be reused
      PreparedStatement stmt = conn.prepareStatement("INSERT INTO \"" + BASE_TABLE_NAME + "\" " +
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)");

      // insert one row
      Timestamp timestampVal1 = new Timestamp(System.currentTimeMillis());
      byte byteVal1 = Byte.MIN_VALUE;
      short shortVal1 = Short.MIN_VALUE;
      int intVal1 = Integer.MIN_VALUE;
      long longVal1 = Long.MIN_VALUE;
      byte[] bytesVal1 = new byte[]{1, 2, 3, 4};
      boolean boolVal1 = false;
      float floatVal1 = Float.MIN_VALUE;
      double doubleVal1 = Double.MIN_VALUE;
      BigDecimal bigDecimalVal1 = new BigDecimal("1234567890.123456");
      insertRow(stmt, stringVal, timestampVal1, byteVal1, shortVal1, intVal1, longVal1, bytesVal1
        , boolVal1, floatVal1, doubleVal1, bigDecimalVal1);

      // insert second row
      Timestamp timestampVal2 = new Timestamp(System.currentTimeMillis());
      byte byteVal2 = Byte.MAX_VALUE;
      short shortVal2 = Short.MAX_VALUE;
      int intVal2 = Integer.MAX_VALUE;
      long longVal2 = Long.MAX_VALUE;
      byte[] bytesVal2 = new byte[]{4, 3, 2, 4};
      boolean boolVal2 = true;
      float floatVal2 = Float.MAX_VALUE;
      double doubleVal2 = Double.MAX_VALUE;
      BigDecimal bigDecimalVal2 = new BigDecimal("9999.999999");
      insertRow(stmt, stringVal, timestampVal2, byteVal2, shortVal2, intVal2, longVal2, bytesVal2
        , boolVal2, floatVal2, doubleVal2, bigDecimalVal2);

      // validate rows are returned in reverse order
      String sql = "SELECT * FROM \"" + BASE_TABLE_NAME +
        "\" WHERE STRING_COL='ACCOUNT1' ORDER BY UNIXTIME_MICROS_COL DESC";
      String expectedPlan = "KuduToEnumerableRel\n" +
        "  KuduSortRel(sort0=[$1], dir0=[DESC], groupBySorted=[false])\n" +
        "    KuduFilterRel(ScanToken 1=[STRING_COL EQUAL ACCOUNT1])\n" +
        "      KuduQuery(table=[[kudu, SCHEMA.TABLE]])\n";
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      assertEquals("Unexpected plan ", expectedPlan, plan);

      rs = conn.createStatement().executeQuery(sql);
      assertTrue(rs.next());
      validateRow(stringVal, timestampVal2, byteVal2, shortVal2, intVal2, longVal2, boolVal2,
        doubleVal2, bigDecimalVal2, rs);
      assertTrue(rs.next());
      validateRow(stringVal, timestampVal1, byteVal1, shortVal1, intVal1, longVal1, boolVal1,
        doubleVal1, bigDecimalVal1, rs);
      assertFalse(rs.next());
    }
  }

  private void validateRow(String stringVal, Timestamp timestampVal, byte byteVal,
                           short shortVal, int intVal, long longVal, boolean boolVal,
                           double doubleVal, BigDecimal bigDecimalVal, ResultSet rs) throws SQLException {
    assertEquals(stringVal, rs.getString(1));
    assertEquals(timestampVal, rs.getTimestamp(2));
    assertEquals(byteVal, rs.getByte(3));
    assertEquals(shortVal, rs.getShort(4));
    assertEquals(intVal, rs.getInt(5));
    assertEquals(longVal, rs.getLong(6));
    // TODO figure out why this isn't working
    // assertArrayEquals(bytesVal1, rs.getBytes(7));
    assertEquals(boolVal, rs.getBoolean(8));
    // TODO this fails because Calcite maps the FLOAT sql type to Java double
    // see https://issues.apache.org/jira/browse/CALCITE-638
    // https://github.com/apache/calcite-avatica/blob/bf6f8f7ef7cf3086ee1a696585a15e7b76120f08/core/src/main/java/org/apache/calcite/avatica/util/AbstractCursor.java#L117
    // assertEquals(floatVal1, rs.getFloat(9), 0.00001f);
    assertEquals(doubleVal, rs.getDouble(10), 0.00001d);
    assertEquals(bigDecimalVal, rs.getBigDecimal(11));
  }

  private void insertRow(PreparedStatement stmt, String stringVal, Timestamp timestampVal,
                         byte byteVal, short shortVal, int intVal, long longVal,
                         byte[] bytesVal, boolean boolVal, float floatVal, double doubleVal,
                         BigDecimal bigDecimalVal) throws SQLException {
    stmt.setString(1, stringVal);
    stmt.setTimestamp(2, timestampVal);
    stmt.setByte(3, byteVal);
    stmt.setShort(4, shortVal);
    stmt.setInt(5, intVal);
    stmt.setLong(6, longVal);
    stmt.setBytes(7, bytesVal);
    stmt.setBoolean(8, boolVal);
    stmt.setFloat(9, floatVal);
    stmt.setDouble(10, doubleVal);
    stmt.setBigDecimal(11, bigDecimalVal);
    stmt.execute();
  }

  @Ignore("Upgrade to calcite 1.22")
  @Test
  public void testInsert() throws Exception {
    String url = String.format(CUSTOM_TEMPLATE,
      testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      long currentTime = System.currentTimeMillis();
      // the TIMESTAMP function truncates the milliseconds
      long expectedTime = currentTime / 1000 * 1000;
      TimestampString ts = TimestampString.fromMillisSinceEpoch(currentTime);
      // insert one row
      String sql = "INSERT INTO \"" + BASE_TABLE_NAME + "\"(STRING_COL, UNIXTIME_MICROS_COL, INT8_COL)" +
        " VALUES ('ACCOUNT2', TIMESTAMP '" + ts + "', 1)";
      conn.createStatement().execute(sql);
      // validate row was written
      ResultSet rs = conn.createStatement().executeQuery("SELECT STRING_COL, UNIXTIME_MICROS_COL, INT8_COL " +
        "FROM \"" + BASE_TABLE_NAME + "\" WHERE STRING_COL='ACCOUNT2'");
      assertTrue(rs.next());
      assertEquals("ACCOUNT2", rs.getString(1));
      assertEquals(new Timestamp(expectedTime), rs.getTimestamp(2));
      assertEquals(1, rs.getInt(3));
      assertFalse(rs.next());

      // insert the same row again
      try {
        conn.createStatement().execute(sql);
        fail("Inserting same row twice should fail.");
      }
      catch (Exception e) {
      }

      // insert a row with a missing non-nullable column value fails
      try {
        sql = "INSERT INTO \"" + BASE_TABLE_NAME + "\"(STRING_COL, UNIXTIME_MICROS_COL)" +
          " VALUES ('ACCOUNT2', TIMESTAMP '" + ts + "')";
        conn.createStatement().execute(sql);
        fail("Inserting a row without a non-nullable column value should fail.");
      }
      catch (SQLException e) {
      }

      // insert a row with column value of wrong type
      try {
        sql = "INSERT INTO \"" + BASE_TABLE_NAME + "\"(STRING_COL, UNIXTIME_MICROS_COL, " +
          "INT8_COL)" +
          " VALUES ('ACCOUNT2', TIMESTAMP '" + ts + "', 'a')";
        conn.createStatement().execute(sql);
        fail("Inserting a row with column value of wrong type should fail.");
      }
      catch (SQLException e) {
      }

      // insert a row with column value that can be coerced to the correct type
      sql = "INSERT INTO \"" + BASE_TABLE_NAME + "\"(STRING_COL, UNIXTIME_MICROS_COL, INT8_COL, " +
        "FLOAT_COL, DECIMAL_COL) VALUES ('ACCOUNT2', TIMESTAMP '" + ts + "', '10', '123.123', " +
        "'1234567890.123456')";
      conn.createStatement().execute(sql);
    }
  }

  @Ignore("TODO enable once we handle RexCalls")
  @Test
  public void testInsertSelect() throws Exception {
    String url = String.format(CUSTOM_TEMPLATE,
      testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      long currentTime = System.currentTimeMillis();
      // the TIMESTAMP function truncates the milliseconds
      long expectedTime = currentTime / 1000 * 1000;
      TimestampString ts = TimestampString.fromMillisSinceEpoch(currentTime);
      // insert one row
      String sql = "INSERT INTO \"" + BASE_TABLE_NAME + "\"(STRING_COL, UNIXTIME_MICROS_COL, INT8_COL)" +
        " VALUES ('ACCOUNT2', CURRENT_TIME, 1)";
      conn.createStatement().execute(sql);

      // run an insert that selects from the same table
      sql = "INSERT INTO \"" + BASE_TABLE_NAME + "\"(STRING_COL, UNIXTIME_MICROS_COL, INT8_COL)" +
        " SELECT STRING_COL, CURRENT_TIME, 2*INT8_COL FROM \"" + BASE_TABLE_NAME + "\"";
      conn.createStatement().execute(sql);
    }

  }

}
