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
package com.twilio.kudu.sql;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.twilio.kudu.sql.metadata.KuduTableMetadata;
import com.twilio.kudu.sql.schema.BaseKuduSchemaFactory;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class DescendingSortedWithNonDateTimeFieldsIT {
  private static String ACCOUNT_SID = "AC1234567";

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();
  private static final String descendingSortTableName = "DescendingSortTestTable";
  private static String JDBC_URL;

  private static void insertRow(PreparedStatement stmt, byte byteVal, short shortVal, int intVal, long longVal)
      throws SQLException {
    stmt.setString(1, ACCOUNT_SID);
    stmt.setByte(2, byteVal);
    stmt.setShort(3, shortVal);
    stmt.setInt(4, intVal);
    stmt.setLong(5, longVal);
    stmt.setString(6, "message-body");
    stmt.execute();
  }

  @BeforeClass
  public static void setup() throws Exception {
    final List<ColumnSchema> columns = Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("account_sid", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("reverse_byte_field", Type.INT8).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("reverse_short_field", Type.INT16).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("reverse_int_field", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("reverse_long_field", Type.INT64).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("resource_type", Type.STRING).build());

    Schema schema = new Schema(columns);

    testHarness.getClient().createTable(descendingSortTableName, schema, new org.apache.kudu.client.CreateTableOptions()
        .addHashPartitions(Arrays.asList("account_sid"), 5).setNumReplicas(1));

    JDBC_URL = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED, KuduSchemaFactory.class.getName(),
        testHarness.getMasterAddressesAsString());

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      // Create prepared statement that can be reused
      PreparedStatement stmt = conn
          .prepareStatement("INSERT INTO \"" + descendingSortTableName + "\" " + "VALUES (?,?,?,?,?,?)");

      // write rows that store the range of possible values
      insertRow(stmt, Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE);
      insertRow(stmt, (byte) (Byte.MIN_VALUE + 1), (short) (Short.MIN_VALUE + 1), Integer.MIN_VALUE + 1,
          Long.MIN_VALUE + 1);
      insertRow(stmt, (byte) -1, (short) -1, -1, -1);
      insertRow(stmt, (byte) 0, (short) 0, 0, 0);
      insertRow(stmt, (byte) 1, (short) 1, 1, 1);
      insertRow(stmt, (byte) (Byte.MAX_VALUE - 1), (short) (Short.MAX_VALUE - 1), Integer.MAX_VALUE - 1,
          Long.MAX_VALUE - 1);
      insertRow(stmt, Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE);
      conn.commit();
    }
  }

  public static class KuduSchemaFactory extends BaseKuduSchemaFactory {
    private static final Map<String, KuduTableMetadata> kuduTableConfigMap = new ImmutableMap.Builder<String, KuduTableMetadata>()
        .put("DescendingSortTestTable",
            new KuduTableMetadata.KuduTableMetadataBuilder().setDescendingOrderedColumnNames(Lists
                .newArrayList("reverse_byte_field", "reverse_short_field", "reverse_int_field", "reverse_long_field"))
                .build())
        .build();

    // Public singleton, per factory contract.
    public static final KuduSchemaFactory INSTANCE = new KuduSchemaFactory(kuduTableConfigMap);

    public KuduSchemaFactory(Map<String, KuduTableMetadata> kuduTableConfigMap) {
      super(kuduTableConfigMap);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testHarness.getClient().deleteTable(descendingSortTableName);
  }

  @Test
  public void testDescendingSortWithReverseSortedFields() throws Exception {
    String url = String.format(JDBC_URL, testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      String firstBatchSqlFormat = "SELECT * FROM \"DescendingSortTestTable\"" + "WHERE account_sid = '%s' "
          + "ORDER BY account_sid asc, reverse_byte_field desc, reverse_short_field desc, reverse_int_field desc, reverse_long_field desc";
      String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], sort3=[$3], sort4=[$4], "
          + "dir0=[ASC], dir1=[DESC], dir2=[DESC], dir3=[DESC], dir4=[DESC])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567])\n"
          + "      KuduQuery(table=[[kudu, DescendingSortTestTable]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
      assertEquals("Unexpected plan ", expectedPlan, plan);
      rs = conn.createStatement().executeQuery(firstBatchSql);

      assertTrue(rs.next());
      validateRow(rs, Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE);
      assertTrue(rs.next());
      validateRow(rs, (byte) (Byte.MAX_VALUE - 1), (short) (Short.MAX_VALUE - 1), Integer.MAX_VALUE - 1,
          Long.MAX_VALUE - 1);
      assertTrue(rs.next());
      validateRow(rs, (byte) 1, (short) 1, 1, 1);
      assertTrue(rs.next());
      validateRow(rs, (byte) 0, (short) 0, 0, 0);
      assertTrue(rs.next());
      validateRow(rs, (byte) -1, (short) -1, -1, -1);
      assertTrue(rs.next());
      validateRow(rs, (byte) (Byte.MIN_VALUE + 1), (short) (Short.MIN_VALUE + 1), Integer.MIN_VALUE + 1,
          Long.MIN_VALUE + 1);
      assertTrue(rs.next());
      validateRow(rs, Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE);
      assertFalse(rs.next());
    }
  }

  private void validateRow(ResultSet rs, byte byteVal, short shortVal, int intVal, long longVal) throws SQLException {
    assertEquals("Mismatched byte", byteVal, rs.getByte("reverse_byte_field"));
    assertEquals("Mismatched short", shortVal, rs.getShort("reverse_short_field"));
    assertEquals("Mismatched int", intVal, rs.getInt("reverse_int_field"));
    assertEquals("Mismatched long", longVal, rs.getLong("reverse_long_field"));
    System.out.println();
  }

  @Test
  public void testAscendingSortWithReverseSortedFields() throws Exception {
    String url = String.format(JDBC_URL, testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      String firstBatchSqlFormat = "SELECT * FROM \"DescendingSortTestTable\""
          + "ORDER BY account_sid, reverse_byte_field, reverse_short_field, reverse_int_field, reverse_long_field asc";
      String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "EnumerableSort(sort0=[$0], sort1=[$1], sort2=[$2], sort3=[$3], sort4=[$4], dir0=[ASC], dir1=[ASC], dir2=[ASC], dir3=[ASC], dir4=[ASC])\n"
          + "  KuduToEnumerableRel\n" + "    KuduQuery(table=[[kudu, DescendingSortTestTable]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
      assertEquals("Unexpected plan ", expectedPlan, plan);
      rs = conn.createStatement().executeQuery(firstBatchSql);

      assertTrue(rs.next());
      validateRow(rs, Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE);
      assertTrue(rs.next());
      validateRow(rs, (byte) (Byte.MIN_VALUE + 1), (short) (Short.MIN_VALUE + 1), Integer.MIN_VALUE + 1,
          Long.MIN_VALUE + 1);
      assertTrue(rs.next());
      validateRow(rs, (byte) -1, (short) -1, -1, -1);
      assertTrue(rs.next());
      validateRow(rs, (byte) 0, (short) 0, 0, 0);
      assertTrue(rs.next());
      validateRow(rs, (byte) 1, (short) 1, 1, 1);
      assertTrue(rs.next());
      validateRow(rs, (byte) (Byte.MAX_VALUE - 1), (short) (Short.MAX_VALUE - 1), Integer.MAX_VALUE - 1,
          Long.MAX_VALUE - 1);
      assertTrue(rs.next());
      validateRow(rs, Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE);
      assertFalse(rs.next());
    }
  }

  @Test
  public void testDescendingSortWithFilter() throws Exception {
    String url = String.format(JDBC_URL, testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      String sqlFormat = "SELECT * FROM \"DescendingSortTestTable\" WHERE " + "account_sid = '%s'"
          + " and reverse_byte_field > CAST(1 AS TINYINT)" + " and reverse_short_field > CAST(1 AS SMALLINT)"
          + " and reverse_int_field > 1" + " and reverse_long_field > CAST(1 AS BIGINT)"
          + " ORDER BY account_sid asc, reverse_byte_field desc, reverse_short_field desc, reverse_int_field desc, reverse_long_field desc";
      String sql = String.format(sqlFormat, ACCOUNT_SID);

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], sort3=[$3], sort4=[$4], dir0=[ASC], dir1=[DESC], dir2=[DESC], dir3=[DESC], dir4=[DESC])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567, reverse_byte_field GREATER 1, reverse_short_field GREATER 1, reverse_int_field GREATER 1, reverse_long_field GREATER 1])\n"
          + "      KuduQuery(table=[[kudu, DescendingSortTestTable]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
      assertEquals("Unexpected plan ", expectedPlan, plan);

      rs = conn.createStatement().executeQuery(sql);
      assertTrue(rs.next());
      validateRow(rs, Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE);
      assertTrue(rs.next());
      validateRow(rs, (byte) (Byte.MAX_VALUE - 1), (short) (Short.MAX_VALUE - 1), Integer.MAX_VALUE - 1,
          Long.MAX_VALUE - 1);
      assertFalse(rs.next());
    }
  }

  @Test
  public void testFilterUsingMinMaxValue() throws Exception {
    String url = String.format(JDBC_URL, testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      String sqlFormat = "SELECT * FROM \"DescendingSortTestTable\" WHERE account_sid = '%s'"
          + " and reverse_byte_field = CAST(%d AS TINYINT)" + " and reverse_short_field = CAST(%d AS SMALLINT)"
          + " and reverse_int_field = %d" + " and reverse_long_field = CAST(%d AS BIGINT)";

      String sql = String.format(sqlFormat, ACCOUNT_SID, Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE,
          Long.MIN_VALUE);
      ResultSet rs = conn.createStatement().executeQuery(sql);
      assertTrue(rs.next());
      validateRow(rs, Byte.MIN_VALUE, Short.MIN_VALUE, Integer.MIN_VALUE, Long.MIN_VALUE);
      assertFalse(rs.next());

      sql = String.format(sqlFormat, ACCOUNT_SID, Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE);
      rs = conn.createStatement().executeQuery(sql);
      assertTrue(rs.next());
      validateRow(rs, Byte.MAX_VALUE, Short.MAX_VALUE, Integer.MAX_VALUE, Long.MAX_VALUE);
      assertFalse(rs.next());
    }

  }
}
