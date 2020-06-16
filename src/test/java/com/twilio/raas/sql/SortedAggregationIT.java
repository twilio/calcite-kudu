package com.twilio.raas.sql;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.twilio.kudu.metadata.KuduTableMetadata;
import com.twilio.raas.sql.schema.KuduTestSchemaFactoryBase;
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

  @BeforeClass
  public static void setup() throws Exception {
    System.setProperty("org.codehaus.janino.source_debugging.enable", "true");
    final List<ColumnSchema> columns = Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("ACCOUNT_SID", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("REVERSE_BYTE_FIELD", Type.INT8).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("REVERSE_SHORT_FIELD", Type.INT16).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("REVERSE_INT_FIELD", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("REVERSE_LONG_FIELD", Type.INT64).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("RESOURCE_TYPE", Type.STRING).build());

    Schema schema = new Schema(columns);

    testHarness.getClient().createTable(descendingSortTableName, schema,
        new org.apache.kudu.client.CreateTableOptions()
            .addHashPartitions(Arrays.asList("ACCOUNT_SID"), 5)
            .setNumReplicas(1));

    JDBC_URL = String.format(JDBCUtil.CALCITE_TEST_MODEL_TEMPLATE,
      KuduTestSchemaFactory.class.getName(), testHarness.getMasterAddressesAsString());

    // insert rows
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      PreparedStatement stmt = conn.prepareStatement("INSERT INTO \"" + descendingSortTableName + "\" " +
        "VALUES (?,?,?,?,?,?)");
      insertRow(stmt, (byte)4, new Short("32"), 100, 1000L, "message-body");
      insertRow(stmt, (byte)4, new Short("33"), 101, 1001L, "message-body");
      insertRow(stmt, (byte)5, new Short("32"), 100, 50L, "message-body");
      insertRow(stmt, (byte)5, new Short("32"), 101, 30L, "message-body");
      insertRow(stmt, (byte)6, new Short("32"), 100, 1001L, "user-login");
      insertRow(stmt, (byte)6, new Short("33"), 101, 1001L, "user-login");
      conn.commit();
    }

  }

  public static class KuduTestSchemaFactory extends KuduTestSchemaFactoryBase {
    private static final Map<String, KuduTableMetadata> kuduTableConfigMap =
      new ImmutableMap.Builder<String, KuduTableMetadata>()
        .put(descendingSortTableName,
          new KuduTableMetadata.KuduTableMetadataBuilder()
            .setDescendingOrderedColumnNames(Lists.newArrayList("REVERSE_BYTE_FIELD",
              "REVERSE_SHORT_FIELD", "REVERSE_INT_FIELD", "REVERSE_LONG_FIELD"))
            .build()
        )
        .build();

    // Public singleton, per factory contract.
    public static final DescendingSortedWithNonDateTimeFieldsIT.KuduTestSchemaFactory INSTANCE =
      new DescendingSortedWithNonDateTimeFieldsIT.KuduTestSchemaFactory(kuduTableConfigMap);

    public KuduTestSchemaFactory(Map<String, KuduTableMetadata> kuduTableConfigMap) {
      super(kuduTableConfigMap);
    }
  }

  private static void insertRow(PreparedStatement stmt, byte byteVal, short shortVal, int intVal,
                                long longVal, String resourceType) throws SQLException {
    stmt.setString(1, JDBCQueryIT.ACCOUNT_SID);
    stmt.setByte(2, byteVal);
    stmt.setShort(3, shortVal);
    stmt.setInt(4, intVal);
    stmt.setLong(5, longVal);
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
        "SELECT account_sid, sum(reverse_long_field), sum(reverse_int_field) FROM %s WHERE " +
                "resource_type = 'message-body' GROUP BY account_sid ORDER BY account_sid ASC " +
                "limit 1",
        descendingSortTableName);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet queryResult = conn.createStatement().executeQuery("SELECT * FROM " + descendingSortTableName);
      queryResult = conn.createStatement().executeQuery(sql);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      final String expectedPlan =
        "EnumerableAggregate(group=[{0}], EXPR$1=[$SUM0($1)], EXPR$2=[$SUM0($2)])\n" +
        "  KuduToEnumerableRel\n" +
        "    KuduSortRel(sort0=[$0], dir0=[ASC], fetch=[1], groupBySorted=[true])\n" +
        "      KuduProjectRel(ACCOUNT_SID=[$0], REVERSE_LONG_FIELD=[$4], REVERSE_INT_FIELD=[$3])\n" +
        "        KuduFilterRel(ScanToken 1=[RESOURCE_TYPE EQUAL message-body])\n" +
        "          KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      assertTrue("Should have results to iterate over",
          queryResult.next());
      assertTrue(String.format("Plan should contain KuduSortRel. It is\n%s", plan),
          plan.contains("KuduSortRel"));
      assertTrue(String.format("KuduSortRel should have groupBySorted set to true. It doesn't\n%s", plan),
          plan.contains("groupBySorted=[true]"));
      assertEquals("Full SQL plan has changed\n",
          expectedPlan, plan);
      assertEquals("Incorrect aggregated value", 2081, queryResult.getLong(2));
      assertFalse("Should not have any more results",
          queryResult.next());
    }
  }

  @Test
  public void aggregateSortedResultsByAccountAndByte() throws Exception {
    final String sql = String.format(
        "SELECT account_sid, sum(cast(reverse_long_field as bigint)) \"reverse_long_field\" FROM %s WHERE resource_type = 'message-body' GROUP BY reverse_byte_field, account_sid ORDER BY account_sid ASC, reverse_byte_field DESC limit 2",
        descendingSortTableName);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      final String expectedPlan =
        "EnumerableCalc(expr#0..2=[{inputs}], ACCOUNT_SID=[$t1], reverse_long_field=[$t2], REVERSE_BYTE_FIELD=[$t0])\n" +
          "  EnumerableAggregate(group=[{0, 1}], reverse_long_field=[$SUM0($2)])\n" +
          "    KuduToEnumerableRel\n" +
          "      KuduSortRel(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC], fetch=[2], groupBySorted=[true])\n" +
          "        KuduProjectRel(REVERSE_BYTE_FIELD=[$1], ACCOUNT_SID=[$0], $f2=[$4])\n" +
          "          KuduFilterRel(ScanToken 1=[RESOURCE_TYPE EQUAL message-body])\n" +
          "            KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      ResultSet queryResult = conn.createStatement().executeQuery(sql);

      assertTrue("Should have results to iterate over",
          queryResult.next());
      assertTrue(String.format("Plan should contain KuduSortRel. It is\n%s", plan),
          plan.contains("KuduSortRel"));
      assertTrue(String.format("KuduSortRel should have groupBySorted set to true. It doesn't\n%s", plan),
          plan.contains("groupBySorted=[true]"));
      assertEquals("Full SQL plan has changed\n",
          expectedPlan, plan);
      assertEquals("Incorrect aggregated value", 80, queryResult.getLong(2));
      assertTrue("Should have more results",
        queryResult.next());
      assertEquals("Incorrect aggregated value", 2001, queryResult.getLong(2));
      assertFalse("Should not have any more results",
        queryResult.next());
    }
  }

  @Test
  public void aggregateSortedResultsByAccountWrongDirection() throws Exception {
    final String sql = String.format(
        "SELECT account_sid, sum(reverse_long_field) FROM %s WHERE resource_type = 'message-body' GROUP BY account_sid ORDER BY account_sid DESC limit 1",
        descendingSortTableName);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      final String expectedPlan =
        "EnumerableLimit(fetch=[1])\n" +
        "  EnumerableSort(sort0=[$0], dir0=[DESC])\n" +
        "    EnumerableAggregate(group=[{0}], EXPR$1=[$SUM0($1)])\n" +
        "      KuduToEnumerableRel\n" +
        "        KuduProjectRel(ACCOUNT_SID=[$0], REVERSE_LONG_FIELD=[$4])\n" +
        "          KuduFilterRel(ScanToken 1=[RESOURCE_TYPE EQUAL message-body])\n" +
        "            KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      ResultSet queryResult = conn.createStatement().executeQuery(sql);

      assertTrue("Should have results to iterate over",
          queryResult.next());
      assertFalse(String.format("Plan should not contain KuduSortRel. It is\n%s", plan),
          plan.contains("KuduSortRel"));
      assertEquals("Full SQL plan has changed\n",
          expectedPlan, plan);
      assertEquals("Incorrect aggregated value", 2081, queryResult.getLong(2));
      assertFalse("Should not have any more results",
          queryResult.next());
    }
  }

  @Test
  public void aggregateSortedResultsByAccountAndByteWithOffset() throws Exception {
    final String sql = String.format(
        "SELECT account_sid, sum(cast(reverse_long_field as bigint)) \"reverse_long_field\" FROM %s WHERE account_sid = '%s' GROUP BY reverse_byte_field, account_sid ORDER BY account_sid ASC, reverse_byte_field DESC limit 1 offset 1",
        descendingSortTableName, JDBCQueryIT.ACCOUNT_SID);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      final String expectedPlan =
        "EnumerableCalc(expr#0..2=[{inputs}], ACCOUNT_SID=[$t1], reverse_long_field=[$t2], REVERSE_BYTE_FIELD=[$t0])\n" +
          "  EnumerableAggregate(group=[{0, 1}], reverse_long_field=[$SUM0($2)])\n" +
          "    KuduToEnumerableRel\n" +
          "      KuduSortRel(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC], offset=[1], fetch=[1], groupBySorted=[true])\n" +
          "        KuduProjectRel(REVERSE_BYTE_FIELD=[$1], ACCOUNT_SID=[$0], $f2=[$4])\n" +
          "          KuduFilterRel(ScanToken 1=[ACCOUNT_SID EQUAL AC1234567])\n" +
          "            KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      ResultSet queryResult = conn.createStatement().executeQuery(sql);

      assertTrue("Should have results to iterate over",
          queryResult.next());
      assertTrue(String.format("Plan should contain KuduSortRel. It is\n%s", plan),
          plan.contains("KuduSortRel"));
      assertTrue(String.format("KuduSortRel should have groupBySorted set to true. It doesn't\n%s", plan),
          plan.contains("groupBySorted=[true]"));
      assertEquals("Full SQL plan has changed\n",
          expectedPlan, plan);
      assertEquals("Incorrect aggregated value", 80, queryResult.getLong(2));
      assertFalse("Should not have any more results",
          queryResult.next());
    }
  }

  @Test
  public void aggregatedResultsGroupedByOutOfOrder() throws Exception {
    final String sql = String.format(
        "SELECT account_sid, sum(reverse_long_field) FROM %s WHERE resource_type = 'message-body'" +
                " GROUP BY account_sid, reverse_int_field ORDER BY account_sid ASC, " +
                "reverse_int_field ASC limit 1",
        descendingSortTableName);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      final String expectedPlan =
        "EnumerableCalc(expr#0..2=[{inputs}], ACCOUNT_SID=[$t0], EXPR$1=[$t2], " +
                "REVERSE_INT_FIELD=[$t1])\n" +
        "  EnumerableLimit(fetch=[1])\n" +
        "    EnumerableSort(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n" +
        "      EnumerableAggregate(group=[{0, 1}], EXPR$1=[$SUM0($2)])\n" +
        "        KuduToEnumerableRel\n" +
        "          KuduProjectRel(ACCOUNT_SID=[$0], REVERSE_INT_FIELD=[$3], " +
                "REVERSE_LONG_FIELD=[$4])\n" +
        "            KuduFilterRel(ScanToken 1=[RESOURCE_TYPE EQUAL message-body])\n" +
        "              KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      assertFalse(String.format("Plan should not contain KuduSortRel. It is\n%s", plan),
          plan.contains("KuduSortRel"));
      assertEquals("Full SQL plan has changed\n",
          expectedPlan, plan);
    }
  }

  @Test
  public void aggregateSortedResultsByAccountWithLimitOfFour() throws Exception {
    final String sql = String.format(
        "SELECT account_sid, reverse_byte_field, sum(reverse_long_field), sum(reverse_int_field) FROM %s WHERE account_sid = '%s' GROUP BY account_sid, reverse_byte_field ORDER BY account_sid ASC, reverse_byte_field DESC limit 4",
        descendingSortTableName, JDBCQueryIT.ACCOUNT_SID);

    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet queryResult = conn.createStatement().executeQuery(sql);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      final String expectedPlan =
        "EnumerableAggregate(group=[{0, 1}], EXPR$2=[$SUM0($2)], EXPR$3=[$SUM0($3)])\n" +
        "  KuduToEnumerableRel\n" +
        "    KuduSortRel(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[DESC], fetch=[4], groupBySorted=[true])\n" +
        "      KuduProjectRel(ACCOUNT_SID=[$0], REVERSE_BYTE_FIELD=[$1], REVERSE_LONG_FIELD=[$4], REVERSE_INT_FIELD=[$3])\n" +
        "        KuduFilterRel(ScanToken 1=[ACCOUNT_SID EQUAL AC1234567])\n" +
        "          KuduQuery(table=[[kudu, SortedAggregationIT]])\n";

      assertTrue("Should have results to iterate over",
          queryResult.next());
      assertTrue(String.format("Plan should contain KuduSortRel. It is\n%s", plan),
          plan.contains("KuduSortRel"));
      assertTrue(String.format("KuduSortRel should have groupBySorted set to true. It doesn't\n%s", plan),
          plan.contains("groupBySorted=[true]"));

      assertEquals("Should be grouped second by Byte of 6",
          (byte)6, queryResult.getByte(2));
      assertTrue("Should have more results",
          queryResult.next());

      assertEquals("Should be grouped first by Byte of 4",
        (byte)5, queryResult.getByte(2));
      assertTrue("Should have more results",
          queryResult.next());

      assertEquals("Should be grouped first by Byte of 4",
        (byte)4, queryResult.getByte(2));
      assertFalse("Should only have three results",
        queryResult.next());

      assertEquals(String.format("Full SQL plan has changed\n%s", plan),
          expectedPlan, plan);

    }
  }
}
