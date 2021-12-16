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
public final class SortedAggregationIT {

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();
  private static final String descendingSortTableName = "SortedAggregationIT";
  private static String JDBC_URL;

  private static String ACCOUNT_SID1 = "ACCOUNT1";
  private static String ACCOUNT_SID2 = "ACCOUNT2";

  @BeforeClass
  public static void setup() throws Exception {
    System.setProperty("org.codehaus.janino.source_debugging.enable", "true");
    final List<ColumnSchema> columns = Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("ACCOUNT_SID", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("FIELD1", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("FIELD2", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("FIELD3", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("FIELD4", Type.INT64).build(),
        new ColumnSchema.ColumnSchemaBuilder("RESOURCE_TYPE", Type.STRING).build());

    Schema schema = new Schema(columns);

    testHarness.getClient().createTable(descendingSortTableName, schema, new org.apache.kudu.client.CreateTableOptions()
        .addHashPartitions(Arrays.asList("ACCOUNT_SID"), 5).setNumReplicas(1));

    JDBC_URL = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED, KuduSchemaFactory.class.getName(),
        testHarness.getMasterAddressesAsString());

    // insert rows
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      PreparedStatement stmt = conn
          .prepareStatement("INSERT INTO \"" + descendingSortTableName + "\" " + "VALUES (?,?,?,?,?,?)");
      // rows for ACCOUNT_SID1
      insertRow(stmt, 4, 29, 100, 1000L, "message-body", ACCOUNT_SID1);
      insertRow(stmt, 4, 29, 101, 1000L, "message-body", ACCOUNT_SID1);
      insertRow(stmt, 5, 30, 101, 501L, "message-body", ACCOUNT_SID1);
      insertRow(stmt, 5, 30, 102, 1001L, "message-body", ACCOUNT_SID1);
      insertRow(stmt, 5, 31, 99, 50L, "message-body", ACCOUNT_SID1);
      insertRow(stmt, 5, 31, 100, 50L, "message-body", ACCOUNT_SID1);
      insertRow(stmt, 5, 31, 101, 30L, "message-body", ACCOUNT_SID1);
      insertRow(stmt, 5, 31, 102, 30L, "message-body", ACCOUNT_SID1);
      insertRow(stmt, 6, 32, 100, 901L, "user-login", ACCOUNT_SID1);
      insertRow(stmt, 6, 32, 101, 2001L, "user-login", ACCOUNT_SID1);
      insertRow(stmt, 6, 33, 101, 10L, "user-login", ACCOUNT_SID1);
      insertRow(stmt, 6, 33, 102, 20L, "user-login", ACCOUNT_SID1);
      // rows for ACCOUNT_SID2
      insertRow(stmt, 6, 1, 100, 4L, "user-login", ACCOUNT_SID2);
      insertRow(stmt, 6, 2, 101, 3L, "user-login", ACCOUNT_SID2);
      insertRow(stmt, 6, 3, 101, 2L, "user-login", ACCOUNT_SID2);
      insertRow(stmt, 6, 4, 102, 1L, "user-login", ACCOUNT_SID2);
      conn.commit();

      // grouped by (FIELD1, FIELD2), SUM(FIELD4) these become
      // ACCOUNT_SID1
      // (6,33) 30
      // (6,32) 2902
      // (5,31) 160
      // (5,30) 1502
      // (4,29) 2000
      // ACCOUNT_SID2
      // (6,4) 1
      // (6,3) 2
      // (6,2) 3
      // (6,1) 4
    }

  }

  public static class KuduSchemaFactory extends BaseKuduSchemaFactory {
    private static final Map<String, KuduTableMetadata> kuduTableConfigMap = new ImmutableMap.Builder<String, KuduTableMetadata>()
        .put(descendingSortTableName, new KuduTableMetadata.KuduTableMetadataBuilder()
            .setDescendingOrderedColumnNames(Lists.newArrayList("FIELD1", "FIELD2", "FIELD3")).build())
        .build();

    // Public singleton, per factory contract.
    public static final KuduSchemaFactory INSTANCE = new KuduSchemaFactory(kuduTableConfigMap);

    public KuduSchemaFactory(Map<String, KuduTableMetadata> kuduTableConfigMap) {
      super(kuduTableConfigMap);
    }
  }

  private static void insertRow(PreparedStatement stmt, int val1, int val2, int val3, long val4, String resourceType,
      String accountSid) throws SQLException {
    stmt.setString(1, accountSid);
    stmt.setInt(2, val1);
    stmt.setInt(3, val2);
    stmt.setInt(4, val3);
    stmt.setLong(5, val4);
    stmt.setString(6, resourceType);
    stmt.execute();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testHarness.getClient().deleteTable(descendingSortTableName);
  }

  @Test
  public void aggregateSortedResultsByAccount() throws Exception {
    final String sql = String.format(
        "SELECT sum(FIELD4) FROM %s WHERE "
            + "resource_type = 'message-body' GROUP BY account_sid ORDER BY account_sid ASC " + "limit 1",
        descendingSortTableName);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet queryResult = conn.createStatement().executeQuery(sql);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      final String expectedPlan = "EnumerableCalc(expr#0..1=[{inputs}], EXPR$0=[$t1], ACCOUNT_SID=[$t0])\n"
          + "  EnumerableAggregate(group=[{0}], EXPR$0=[$SUM0($1)])\n" + "    KuduToEnumerableRel\n"
          + "      KuduSortRel(sort0=[$0], dir0=[ASC], fetch=[1], groupBySorted=[true])\n"
          + "        KuduProjectRel(ACCOUNT_SID=[$0], FIELD4=[$4])\n"
          + "          KuduFilterRel(ScanToken 1=[RESOURCE_TYPE EQUAL message-body])\n"
          + "            KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      assertTrue("Should have results to iterate over", queryResult.next());
      assertTrue(String.format("Plan should contain KuduSortRel. It is\n%s", plan), plan.contains("KuduSortRel"));
      assertTrue(String.format("KuduSortRel should have groupBySorted set to true. It doesn't\n%s", plan),
          plan.contains("groupBySorted=[true]"));
      assertEquals("Full SQL plan has changed\n", expectedPlan, plan);
      assertEquals("Incorrect aggregated value", 3662, queryResult.getLong(1));
      assertFalse("Should not have any more results", queryResult.next());
    }
  }

  @Test
  public void testAggregationSort() throws Exception {
    String sql = String.format("SELECT FIELD1, FIELD2, SUM(FIELD4) FROM %s " + "WHERE account_sid = '%s' "
        + "GROUP BY (FIELD2, FIELD1) " + "ORDER BY FIELD1 DESC, FIELD2 DESC LIMIT 2 OFFSET 1 ", descendingSortTableName,
        ACCOUNT_SID1);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet queryResult = conn.createStatement().executeQuery(sql);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      String expectedPlan = "EnumerableCalc(expr#0..2=[{inputs}], FIELD1=[$t1], FIELD2=[$t0], EXPR$2=[$t2])\n"
          + "  EnumerableAggregate(group=[{0, 1}], EXPR$2=[$SUM0($2)])\n" + "    KuduToEnumerableRel\n"
          + "      KuduSortRel(sort0=[$1], sort1=[$2], dir0=[DESC], dir1=[DESC], offset=[1], fetch=[2], groupBySorted=[true])\n"
          + "        KuduProjectRel(FIELD2=[$2], FIELD1=[$1], FIELD4=[$4])\n"
          + "          KuduFilterRel(ScanToken 1=[ACCOUNT_SID EQUAL ACCOUNT1])\n"
          + "            KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      assertEquals("Unexpected plan", expectedPlan, plan);
      assertTrue("Should have results to iterate over", queryResult.next());
      assertEquals(6, queryResult.getByte(1));
      assertEquals(32, queryResult.getShort(2));
      assertEquals("Incorrect aggregated value", 2902, queryResult.getLong(3));
      assertTrue(queryResult.next());
      assertEquals(5, queryResult.getByte(1));
      assertEquals(31, queryResult.getShort(2));
      assertEquals("Incorrect aggregated value", 160, queryResult.getLong(3));
      assertFalse("Should not have any more results", queryResult.next());

      // query the next page of results using (5,31) as the page offset
      sql = String.format(
          "SELECT FIELD1, FIELD2, SUM(FIELD4) FROM %s " + "WHERE (account_sid = '%s' AND (FIELD1,FIELD2) > (5, 31) ) "
              + "GROUP BY (FIELD2, FIELD1) ORDER BY FIELD1 DESC, FIELD2 DESC LIMIT 1",
          descendingSortTableName, ACCOUNT_SID1);

      rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      plan = SqlUtil.getExplainPlan(rs);
      queryResult = conn.createStatement().executeQuery(sql);

      expectedPlan = "EnumerableCalc(expr#0..2=[{inputs}], FIELD1=[$t1], FIELD2=[$t0], EXPR$2=[$t2])\n"
          + "  EnumerableAggregate(group=[{0, 1}], EXPR$2=[$SUM0($2)])\n" + "    KuduToEnumerableRel\n"
          + "      KuduSortRel(sort0=[$1], sort1=[$2], dir0=[DESC], dir1=[DESC], fetch=[1], groupBySorted=[true])\n"
          + "        KuduProjectRel(FIELD2=[$2], FIELD1=[$1], FIELD4=[$4])\n"
          + "          KuduFilterRel(ScanToken 1=[ACCOUNT_SID EQUAL ACCOUNT1, FIELD1 LESS 5], ScanToken 2=[ACCOUNT_SID EQUAL ACCOUNT1, FIELD1 EQUAL 5, FIELD2 LESS 31])\n"
          + "            KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      assertEquals("Unexpected plan", expectedPlan, plan);
      assertTrue("Should have results to iterate over", queryResult.next());
      assertEquals(5, queryResult.getByte(1));
      assertEquals(30, queryResult.getShort(2));
      assertEquals("Incorrect aggregated value", 1502, queryResult.getLong(3));
      assertFalse("Should not have any more results", queryResult.next());
    }
  }

  @Test
  public void testAggregationSortByPKPrefix() throws Exception {
    String sql = String.format(
        "SELECT FIELD1, FIELD2, SUM(FIELD4) " + "FROM %s WHERE account_sid = '%s' GROUP BY "
            + "(FIELD2, FIELD1) ORDER BY FIELD1 DESC, SUM(FIELD4) DESC LIMIT 2 OFFSET 1",
        descendingSortTableName, ACCOUNT_SID1);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet queryResult = conn.createStatement().executeQuery(sql);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      String expectedPlan = "EnumerableCalc(expr#0..2=[{inputs}], FIELD1=[$t1], FIELD2=[$t0], EXPR$2=[$t2])\n"
          + "  EnumerableLimitSort(sort0=[$1], sort1=[$2], dir0=[DESC], dir1=[DESC], offset=[1], fetch=[2])\n"
          + "    EnumerableAggregate(group=[{0, 1}], EXPR$2=[$SUM0($2)])\n" + "      KuduToEnumerableRel\n"
          + "        KuduSortRel(sort0=[$1], sort1=[$2], dir0=[DESC], dir1=[DESC], offset=[1], fetch=[2], groupBySorted=[true], sortPkPrefixColumns=[[1 DESC]])\n"
          + "          KuduProjectRel(FIELD2=[$2], FIELD1=[$1], FIELD4=[$4])\n"
          + "            KuduFilterRel(ScanToken 1=[ACCOUNT_SID EQUAL ACCOUNT1])\n"
          + "              KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      assertEquals("Unexpected plan", expectedPlan, plan);
      assertTrue("Should have results to iterate over", queryResult.next());
      assertEquals(6, queryResult.getByte(1));
      assertEquals(33, queryResult.getShort(2));
      assertEquals("Incorrect aggregated value", 30, queryResult.getLong(3));
      assertTrue(queryResult.next());
      assertEquals(5, queryResult.getByte(1));
      assertEquals(30, queryResult.getShort(2));
      assertEquals("Incorrect aggregated value", 1502, queryResult.getLong(3));
      assertFalse("Should not have any more results", queryResult.next());

      sql = String.format(
          "SELECT FIELD1, FIELD2, SUM(FIELD4) " + "FROM %s WHERE account_sid = '%s' GROUP BY "
              + "(FIELD1, FIELD2) ORDER BY FIELD1 DESC, SUM(FIELD4) DESC LIMIT 1",
          descendingSortTableName, ACCOUNT_SID2);
      queryResult = conn.createStatement().executeQuery(sql);
      assertTrue("Should have results to iterate over", queryResult.next());
      assertEquals(6, queryResult.getByte(1));
      assertEquals(1, queryResult.getShort(2));
      assertEquals("Incorrect aggregated value", 4, queryResult.getLong(3));
      assertFalse("Should not have any more results", queryResult.next());
    }
  }

  @Test
  public void testAggregationSortByPKExpression() throws Exception {
    // TODO see if KuduAggregationLimitRule can be modified to support this type of
    // query
    String sql = String.format(
        "SELECT 6-FIELD1, FIELD2, SUM(FIELD4) " + "FROM %s WHERE account_sid = '%s' GROUP BY "
            + "(FIELD2, 6-FIELD1) ORDER BY 6-FIELD1 DESC, SUM(FIELD4) DESC LIMIT 2 OFFSET 1",
        descendingSortTableName, ACCOUNT_SID1);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      String expectedPlan = "EnumerableCalc(expr#0..2=[{inputs}], EXPR$0=[$t1], FIELD2=[$t0], EXPR$2=[$t2])\n"
          + "  EnumerableLimitSort(sort0=[$1], sort1=[$2], dir0=[DESC], dir1=[DESC], offset=[1], fetch=[2])\n"
          + "    EnumerableAggregate(group=[{0, 1}], EXPR$2=[$SUM0($2)])\n" + "      KuduToEnumerableRel\n"
          + "        KuduProjectRel(FIELD2=[$2], EXPR$0=[-(6, $1)], FIELD4=[$4])\n"
          + "          KuduFilterRel(ScanToken 1=[ACCOUNT_SID EQUAL ACCOUNT1])\n"
          + "            KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      assertEquals("Unexpected plan", expectedPlan, plan);
    }
  }

  @Test
  public void aggregateSortedResultsByAccountAndField1() throws Exception {
    final String sql = String.format("SELECT account_sid, sum(cast(FIELD4 as bigint)) \"FIELD4\" FROM %s WHERE "
        + "resource_type = 'message-body' GROUP BY FIELD1, account_sid ORDER BY account_sid ASC, FIELD1 DESC limit 2",
        descendingSortTableName);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      final String expectedPlan = "EnumerableCalc(expr#0..2=[{inputs}], ACCOUNT_SID=[$t1], FIELD4=[$t2], FIELD1=[$t0])\n"
          + "  EnumerableAggregate(group=[{0, 1}], FIELD4=[$SUM0($2)])\n" + "    KuduToEnumerableRel\n"
          + "      KuduSortRel(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC], fetch=[2], groupBySorted=[true])\n"
          + "        KuduProjectRel(FIELD1=[$1], ACCOUNT_SID=[$0], $f2=[$4])\n"
          + "          KuduFilterRel(ScanToken 1=[RESOURCE_TYPE EQUAL message-body])\n"
          + "            KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      ResultSet queryResult = conn.createStatement().executeQuery(sql);

      assertTrue("Should have results to iterate over", queryResult.next());
      assertTrue(String.format("Plan should contain KuduSortRel. It is\n%s", plan), plan.contains("KuduSortRel"));
      assertTrue(String.format("KuduSortRel should have groupBySorted set to true. It doesn't\n%s", plan),
          plan.contains("groupBySorted=[true]"));
      assertEquals("Full SQL plan has changed\n", expectedPlan, plan);
      assertEquals("Incorrect aggregated value", 1662, queryResult.getLong(2));
      assertTrue("Should have more results", queryResult.next());
      assertEquals("Incorrect aggregated value", 2000, queryResult.getLong(2));
      assertFalse("Should not have any more results", queryResult.next());
    }
  }

  @Test
  public void aggregateSortedResultsByAccountWrongDirection() throws Exception {
    final String sql = String
        .format("SELECT account_sid, sum(FIELD4) FROM %s WHERE resource_type = 'message-body' GROUP BY "
            + "account_sid ORDER BY account_sid DESC limit 1", descendingSortTableName);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      final String expectedPlan = "EnumerableLimitSort(sort0=[$0], dir0=[DESC], fetch=[1])\n"
          + "  EnumerableAggregate(group=[{0}], EXPR$1=[$SUM0($1)])\n" + "    KuduToEnumerableRel\n"
          + "      KuduProjectRel(ACCOUNT_SID=[$0], FIELD4=[$4])\n"
          + "        KuduFilterRel(ScanToken 1=[RESOURCE_TYPE EQUAL message-body])\n"
          + "          KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      ResultSet queryResult = conn.createStatement().executeQuery(sql);

      assertTrue("Should have results to iterate over", queryResult.next());
      assertFalse(String.format("Plan should not contain KuduSortRel. It is\n%s", plan), plan.contains("KuduSortRel"));
      assertEquals("Full SQL plan has changed\n", expectedPlan, plan);
      assertEquals("Incorrect aggregated value", 3662, queryResult.getLong(2));
      assertFalse("Should not have any more results", queryResult.next());
    }
  }

  @Test
  public void aggregateSortedResultsByAccountAndField1WithOffset() throws Exception {
    final String sql = String.format(
        "SELECT account_sid, sum(cast(FIELD4 as bigint)) \"FIELD4\" FROM %s WHERE account_sid = '%s' GROUP BY FIELD1, account_sid ORDER BY account_sid ASC, FIELD1 DESC limit 1 offset 1",
        descendingSortTableName, ACCOUNT_SID1);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      final String expectedPlan = "EnumerableCalc(expr#0..2=[{inputs}], ACCOUNT_SID=[$t1], FIELD4=[$t2], FIELD1=[$t0])\n"
          + "  EnumerableAggregate(group=[{0, 1}], FIELD4=[$SUM0($2)])\n" + "    KuduToEnumerableRel\n"
          + "      KuduSortRel(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC], offset=[1], fetch=[1], groupBySorted=[true])\n"
          + "        KuduProjectRel(FIELD1=[$1], ACCOUNT_SID=[$0], $f2=[$4])\n"
          + "          KuduFilterRel(ScanToken 1=[ACCOUNT_SID EQUAL ACCOUNT1])\n"
          + "            KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      ResultSet queryResult = conn.createStatement().executeQuery(sql);

      assertTrue("Should have results to iterate over", queryResult.next());
      assertTrue(String.format("Plan should contain KuduSortRel. It is\n%s", plan), plan.contains("KuduSortRel"));
      assertTrue(String.format("KuduSortRel should have groupBySorted set to true. It doesn't\n%s", plan),
          plan.contains("groupBySorted=[true]"));
      assertEquals("Full SQL plan has changed\n", expectedPlan, plan);
      assertEquals("Incorrect aggregated value", 1662, queryResult.getLong(2));
      assertFalse("Should not have any more results", queryResult.next());
    }
  }

  @Test
  public void aggregatedResultsGroupedByOutOfOrder() throws Exception {
    final String sql = String.format(
        "SELECT account_sid, sum(FIELD4) FROM %s WHERE resource_type = 'message-body'"
            + " GROUP BY account_sid, FIELD1 ORDER BY account_sid ASC, " + "FIELD1 ASC" + " limit 1",
        descendingSortTableName);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      final String expectedPlan = "EnumerableCalc(expr#0..2=[{inputs}], ACCOUNT_SID=[$t0], EXPR$1=[$t2], FIELD1=[$t1])\n"
          + "  EnumerableLimitSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], fetch=[1])\n"
          + "    EnumerableAggregate(group=[{0, 1}], EXPR$1=[$SUM0($2)])\n" + "      KuduToEnumerableRel\n"
          + "        KuduSortRel(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC], fetch=[1], groupBySorted=[true], sortPkPrefixColumns=[[0]])\n"
          + "          KuduProjectRel(ACCOUNT_SID=[$0], FIELD1=[$1], FIELD4=[$4])\n"
          + "            KuduFilterRel(ScanToken 1=[RESOURCE_TYPE EQUAL message-body])\n"
          + "              KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      assertEquals("Full SQL plan has changed\n", expectedPlan, plan);
    }
  }

  @Test
  public void aggregateSortedResultsByAccountWithLimitOfFour() throws Exception {
    final String sql = String.format(
        "SELECT account_sid, FIELD1, sum(FIELD3), sum(FIELD4) FROM %s "
            + "WHERE account_sid = '%s' GROUP BY account_sid, FIELD1 ORDER BY account_sid ASC, FIELD1 DESC limit 4",
        descendingSortTableName, ACCOUNT_SID1);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet queryResult = conn.createStatement().executeQuery(sql);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      final String expectedPlan = "EnumerableAggregate(group=[{0, 1}], EXPR$2=[$SUM0($2)], EXPR$3=[$SUM0($3)])\n"
          + "  KuduToEnumerableRel\n"
          + "    KuduSortRel(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC], fetch=[4], groupBySorted=[true])\n"
          + "      KuduProjectRel(ACCOUNT_SID=[$0], FIELD1=[$1], FIELD3=[$3], FIELD4=[$4])\n"
          + "        KuduFilterRel(ScanToken 1=[ACCOUNT_SID EQUAL ACCOUNT1])\n"
          + "          KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      assertTrue("Should have results to iterate over", queryResult.next());
      assertTrue(String.format("Plan should contain KuduSortRel. It is\n%s", plan), plan.contains("KuduSortRel"));
      assertTrue(String.format("KuduSortRel should have groupBySorted set to true. It doesn't\n%s", plan),
          plan.contains("groupBySorted=[true]"));

      assertEquals("Should be grouped second by Byte of 6", 6, queryResult.getByte(2));
      assertTrue("Should have more results", queryResult.next());

      assertEquals("Should be grouped first by Byte of 4", 5, queryResult.getByte(2));
      assertTrue("Should have more results", queryResult.next());

      assertEquals("Should be grouped first by Byte of 4", 4, queryResult.getByte(2));
      assertFalse("Should only have three results", queryResult.next());

      assertEquals(String.format("Full SQL plan has changed\n%s", plan), expectedPlan, plan);

    }
  }

}
