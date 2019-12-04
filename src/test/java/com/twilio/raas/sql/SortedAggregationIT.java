package com.twilio.raas.sql;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SortedAggregationIT {
  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();
  private static final String descendingSortTableName = "DescendingSortTestTable";
  private static final String customTemplate = "jdbc:calcite:model=inline:{version: '1.0',defaultSchema:'kudu',schemas:[{name: 'kudu',type:'custom',factory:'com.twilio.raas.sql.KuduSchemaFactory',operand:{connect:'%s',kuduTableConfigs:[{tableName: 'DescendingSortTestTable', descendingSortedFields:['reverse_byte_field', 'reverse_short_field', 'reverse_int_field', 'reverse_long_field']}]}}]};caseSensitive=false;timeZone=UTC";

  private static KuduTable descendingSortTestTable;

  @BeforeClass
  public static void setup() throws Exception {
    System.setProperty("org.codehaus.janino.source_debugging.enable", "true");
    final List<ColumnSchema> columns = Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("account_sid", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("reverse_byte_field", Type.INT8).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("reverse_short_field", Type.INT16).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("reverse_int_field", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("reverse_long_field", Type.INT64).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("resource_type", Type.STRING).build());

    Schema schema = new Schema(columns);

    testHarness.getClient().createTable(descendingSortTableName, schema,
        new org.apache.kudu.client.CreateTableOptions()
            .addHashPartitions(Arrays.asList("account_sid"), 5)
            .setNumReplicas(1));
    final KuduTable descendingSortTestTable = testHarness.getClient().openTable(descendingSortTableName);
    final AsyncKuduSession insertSession = testHarness.getAsyncClient().newSession();

    final Upsert firstRowOp = descendingSortTestTable.newUpsert();
    final PartialRow firstRowWrite = firstRowOp.getRow();
    firstRowWrite.addString("account_sid", JDBCQueryRunnerIT.ACCOUNT_SID);
    firstRowWrite.addByte("reverse_byte_field", (byte)(Byte.MAX_VALUE - new Byte("4")));
    firstRowWrite.addShort("reverse_short_field", (short)(Short.MAX_VALUE - new Short("32")));
    firstRowWrite.addInt("reverse_int_field", Integer.MAX_VALUE - 100);
    firstRowWrite.addLong("reverse_long_field", Long.MAX_VALUE - 1000L);
    firstRowWrite.addString("resource_type", "message-body");
    insertSession.apply(firstRowOp).join();

    final Upsert secondRowOp = descendingSortTestTable.newUpsert();
    final PartialRow secondRowWrite = secondRowOp.getRow();
    secondRowWrite.addString("account_sid", JDBCQueryRunnerIT.ACCOUNT_SID);
    secondRowWrite.addByte("reverse_byte_field", (byte)(Byte.MAX_VALUE - new Byte("5")));
    secondRowWrite.addShort("reverse_short_field", (short)(Short.MAX_VALUE - new Short("33")));
    secondRowWrite.addInt("reverse_int_field", Integer.MAX_VALUE - 101);
    secondRowWrite.addLong("reverse_long_field", Long.MAX_VALUE - 1001L);
    secondRowWrite.addString("resource_type", "message-body");
    insertSession.apply(secondRowOp).join();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testHarness.getClient().deleteTable(descendingSortTableName);
  }

  @Test
  public void aggregateSortedResultsByAccount() throws Exception {
    final String sql = String.format(
        "SELECT account_sid, sum(reverse_long_field), sum(reverse_int_field) FROM %s WHERE account_sid = '%s' GROUP BY account_sid ORDER BY account_sid ASC limit 1",
        descendingSortTableName, JDBCQueryRunnerIT.ACCOUNT_SID);

    String url = String.format(customTemplate, testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      ResultSet queryResult = conn.createStatement().executeQuery(sql);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      assertTrue("Should have results to iterate over",
          queryResult.next());
      assertTrue(String.format("Plan should contain KuduSortRel. It is\n%s", plan),
          plan.contains("KuduSortRel"));
      assertTrue(String.format("KuduSortRel should have groupByLimited set to true. It doesn't\n%s", plan),
          plan.contains("groupByLimited=[true]"));
      assertTrue(String.format("Stored value should be reversed in sumation %d", queryResult.getLong(2)),
          2001L == queryResult.getLong(2));
      assertFalse("Should not have any more results",
          queryResult.next());
    }
  }

  @Test
  public void aggregateSortedResultsByAccountAndByte() throws Exception {
    final String sql = String.format(
        "SELECT account_sid, sum(cast(reverse_long_field as bigint)) \"reverse_long_field\" FROM %s WHERE account_sid = '%s' GROUP BY reverse_byte_field, account_sid ORDER BY account_sid ASC, reverse_byte_field DESC limit 1",
        descendingSortTableName, JDBCQueryRunnerIT.ACCOUNT_SID);

    String url = String.format(customTemplate, testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      ResultSet queryResult = conn.createStatement().executeQuery(sql);

      assertTrue("Should have results to iterate over",
          queryResult.next());
      assertTrue(String.format("Plan should contain KuduSortRel. It is\n%s", plan),
          plan.contains("KuduSortRel"));
      assertTrue(String.format("KuduSortRel should have groupByLimited set to true. It doesn't\n%s", plan),
          plan.contains("groupByLimited=[true]"));
      assertTrue(String.format("Stored value should be reversed in sumation %d", queryResult.getLong(2)),
          1001L == queryResult.getLong(2));
      assertFalse("Should not have any more results",
          queryResult.next());
    }
  }

  @Test
  public void aggregateSortedResultsByAccountWrongDirection() throws Exception {
    final String sql = String.format(
        "SELECT account_sid, sum(reverse_long_field) FROM %s WHERE account_sid = '%s' GROUP BY account_sid ORDER BY account_sid DESC limit 1",
        descendingSortTableName, JDBCQueryRunnerIT.ACCOUNT_SID);

    String url = String.format(customTemplate, testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);

      ResultSet queryResult = conn.createStatement().executeQuery(sql);

      assertTrue("Should have results to iterate over",
          queryResult.next());
      assertFalse(String.format("Plan should not contain KuduSortRel. It is\n%s", plan),
          plan.contains("KuduSortRel"));
      assertTrue(String.format("Stored value should be reversed in sumation %d", queryResult.getLong(2)),
          2001L == queryResult.getLong(2));
      assertFalse("Should not have any more results",
          queryResult.next());
    }
  }

}
