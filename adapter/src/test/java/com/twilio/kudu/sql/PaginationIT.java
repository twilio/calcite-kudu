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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.twilio.kudu.sql.metadata.KuduTableMetadata;
import com.twilio.kudu.sql.schema.BaseKuduSchemaFactory;
import com.twilio.kudu.sql.schema.DefaultKuduSchemaFactory;
import org.apache.calcite.util.TimestampString;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class PaginationIT {

  public static final String ACCOUNT_SID = "account_sid";
  public static final String DATE_INITIATED = "date_initiated";
  public static final String TRANSACTION_ID = "transaction_id";
  public static final String PHONENUMBER = "phonenumber";

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();

  public static final String ACCOUNT1 = "ACCOUNT1";
  public static final String ACCOUNT2 = "ACCOUNT2";
  public static final long T1 = 1000;
  public static final long T2 = 2000;
  public static final long T3 = 3000;
  public static final long T4 = 4000;
  public static final String[] ACCOUNTS = new String[] { ACCOUNT1, ACCOUNT2 };
  public static final String TEST_ORGANIZATION_SID = "ORcaba0759581b24aad1915c70c866f1bb";
  public static final long[] TIMESTAMP_PARTITIONS = new long[] { T1, T2, T3 };
  public static final int NUM_ROWS_PER_PARTITION = 10;
  public static KuduTable kuduTable;

  private final boolean descending;
  private final String tableName;

  private static String JDBC_URL;

  @Parameterized.Parameters(name = "PaginationIT_descending={0}")
  public static synchronized Collection<Boolean> data() {
    return Arrays.asList(new Boolean[] { false, true });
  }

  public PaginationIT(boolean descending) throws Exception {
    this.descending = descending;
    this.tableName = "TABLE_" + (descending ? "DESC" : "ASC");
  }

  @BeforeClass
  public static void setup() throws Exception {
    KuduClient client = testHarness.getClient();

    createTable(client, "TABLE_ASC", false);
    createTable(client, "TABLE_DESC", true);

    JDBC_URL = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED, KuduSchemaFactory.class.getName(),
        testHarness.getMasterAddressesAsString());
    createOrganizationAccounts(client);
  }

  public static void createOrganizationAccounts(KuduClient kuduClient) throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
         Statement stmt = conn.createStatement()) {
      String ddl =
              "CREATE TABLE \"OrganizationAccounts\" ("
                      + "\"organization_sid\" VARCHAR, "
                      + "\"account_sid\" VARCHAR, "
                      + "PRIMARY KEY (\"organization_sid\", \"account_sid\"))"
                      + "PARTITION BY HASH (\"organization_sid\") PARTITIONS 2 NUM_REPLICAS 1";
      stmt.execute(ddl);
    }
    kuduTable = kuduClient.openTable("OrganizationAccounts");
    KuduSession insertSession = kuduClient.newSession();
    for (String account : ACCOUNTS) {
      insertOrgRow(insertSession, account, TEST_ORGANIZATION_SID);
    }
    insertSession.close();
  }

  private static void insertOrgRow(
          KuduSession insertSession, String usageAccountSid, String organizationSid)
  {
    try {
      Upsert upsert = kuduTable.newUpsert();
      PartialRow row = upsert.getRow();
      row.addString("account_sid", usageAccountSid);
      row.addString("organization_sid", organizationSid);
      insertSession.apply(upsert);
      System.out.println("Minakshi inserted row in org table " + usageAccountSid + " orgsid: " + organizationSid);
    }catch(Exception ex){
      System.out.println("Minakshi caught exception while inserting row" + ex);
    }
  }

  public static class KuduSchemaFactory extends BaseKuduSchemaFactory {
    private static final Map<String, KuduTableMetadata> kuduTableConfigMap = new ImmutableMap.Builder<String, KuduTableMetadata>()
        .put("TABLE_DESC", new KuduTableMetadata.KuduTableMetadataBuilder()
            .setDescendingOrderedColumnNames(Lists.newArrayList("date_initiated")).build())
        .build();

    // Public singleton, per factory contract.
    public static final KuduSchemaFactory INSTANCE = new KuduSchemaFactory(kuduTableConfigMap);

    public KuduSchemaFactory(Map<String, KuduTableMetadata> kuduTableConfigMap) {
      super(kuduTableConfigMap);
    }
  }

  private static Timestamp normalizeTimestamp(boolean descending, long ts) {
    return new Timestamp(descending ? CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS - ts : ts);
  }

  /**
   * Creates a table with 3 hash partitions and 3 date range partitions. Loads 10
   * rows per date parition per (for a total of 30 rows) for two accounts.
   */
  private static void createTable(KuduClient kuduClient, String tableName, boolean descending) throws Exception {
    // create the table
    ColumnTypeAttributes scaleAndPrecision = new ColumnTypeAttributes.ColumnTypeAttributesBuilder().scale(22)
        .precision(6).build();
    final List<ColumnSchema> columns = Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder(ACCOUNT_SID, Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder(DATE_INITIATED, Type.UNIXTIME_MICROS).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder(TRANSACTION_ID, Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder(PHONENUMBER, Type.STRING).build());

    Schema schema = new Schema(columns);
    PartialRow row1 = schema.newPartialRow();
    row1.addTimestamp(DATE_INITIATED, normalizeTimestamp(descending, T1));
    PartialRow row2 = schema.newPartialRow();
    row2.addTimestamp(DATE_INITIATED, normalizeTimestamp(descending, T2));
    PartialRow row3 = schema.newPartialRow();
    row3.addTimestamp(DATE_INITIATED, normalizeTimestamp(descending, T3));
    PartialRow row4 = schema.newPartialRow();
    row3.addTimestamp(DATE_INITIATED, normalizeTimestamp(descending, T4));

    // create a table with 3 hash partitions and 3 range partitions for a total of 9
    // tablets
    CreateTableOptions tableBuilder = new CreateTableOptions();
    tableBuilder.addHashPartitions(Arrays.asList(ACCOUNT_SID), 3).setNumReplicas(1);
    if (descending) {
      tableBuilder.setRangePartitionColumns(ImmutableList.of(DATE_INITIATED)).addRangePartition(row4, row3)
          .addRangePartition(row3, row2).addRangePartition(row2, row1);
    } else {
      tableBuilder.setRangePartitionColumns(ImmutableList.of(DATE_INITIATED)).addRangePartition(row1, row2) // [1000,2000)
          .addRangePartition(row2, row3) // [2000,3000)
          .addRangePartition(row3, row4); // [3000,4000)
    }
    kuduClient.createTable(tableName, schema, tableBuilder);

    KuduTable kuduTable = kuduClient.openTable(tableName);
    KuduSession insertSession = kuduClient.newSession();
    // insert 10 rows for each range partition
    for (String account : ACCOUNTS) {
      for (long timestampPartition : TIMESTAMP_PARTITIONS) {
        int counter = 0;
        for (int i = 0; i < NUM_ROWS_PER_PARTITION; ++i) {
          insertRow(kuduTable, insertSession, account, normalizeTimestamp(descending, timestampPartition + 1),
              counter++);
        }
      }
    }
  }

  private static void insertRow(KuduTable kuduTable, KuduSession insertSession, String accountSid,
      Timestamp dateInitiated, int id) throws Exception {
    Upsert upsert = kuduTable.newUpsert();
    PartialRow row = upsert.getRow();
    row.addString(ACCOUNT_SID, accountSid);
    row.addTimestamp(DATE_INITIATED, dateInitiated);
    row.addString(TRANSACTION_ID, "TXN" + (id));
    row.addString(PHONENUMBER, "512-123-123" + (id % 2));
    OperationResponse op = insertSession.apply(upsert);
  }

  public static Object[] validateRow(ResultSet rs, long expectedTimestamp, String expectedTransactionId,
      String expectedAccountSid) throws SQLException {
    String accountSid = rs.getString(ACCOUNT_SID);
    String transactionId = rs.getString(3);
    long timestamp = rs.getTimestamp(DATE_INITIATED).toInstant().toEpochMilli();
    if (expectedAccountSid != null) {
      assertEquals("Mismatched usage account sid", expectedAccountSid, rs.getString(ACCOUNT_SID));
    }
    assertEquals("Mismatched date initiated", expectedTimestamp + 1, timestamp);
    assertEquals("Mismatched transaction id", expectedTransactionId, transactionId);
//    System.out.println(rs.getString(ACCOUNT_SID) + " " + rs.getTimestamp(DATE_INITIATED) + " " + rs.getString(TRANSACTION_ID));
    return new Object[] { accountSid, timestamp, transactionId };
  }

  @Test
  public void testQueryNoRows() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      TimestampString timestampString = TimestampString.fromMillisSinceEpoch(100);
      String sqlFormat = "SELECT * FROM %s WHERE account_sid = '%s' AND " + "date_initiated < TIMESTAMP'%s'";
      String sql = String.format(sqlFormat, tableName, ACCOUNT1, timestampString);
      String expectedPlan = String.format(
          "KuduToEnumerableRel\n" + "  KuduFilterRel(ScanToken 1=[account_sid EQUAL %s, date_initiated LESS 100000])"
              + "\n    KuduQuery(table=[[kudu, %s]])\n",
          ACCOUNT1, tableName);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      assertEquals("Unexpected plan ", expectedPlan, plan);
      rs = conn.createStatement().executeQuery(sql);
      assertFalse(rs.next());
    }
  }

  @Test
  public void testLimit() throws Exception {
    String url = String.format(JDBC_URL, testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      String sql = String.format("SELECT * FROM %s LIMIT 3", tableName);
      String expectedPlan = String.format(
          "KuduToEnumerableRel\n" + "  KuduLimitRel(limit=[3])\n" + "    KuduQuery(table=[[kudu, %s]])\n", tableName);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      assertEquals("Unexpected plan ", expectedPlan, plan);

      // since the rows are not ordered just assert that we get the expected number of
      // rows
      rs = conn.createStatement().executeQuery(sql);
      assertTrue(rs.next());
      assertTrue(rs.next());
      assertTrue(rs.next());
      assertFalse(rs.next());
    }
  }

  @Test
  public void testFilterWithLimitAndOffset() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      // this query will return rows in an unpredictable order
      String sqlFormat = "SELECT * FROM %s " + "WHERE account_sid = '%s' " + "LIMIT 20 OFFSET 5";
      String sql = String.format(sqlFormat, tableName, ACCOUNT1);

      // verify plan
      String expectedPlanFormat = "KuduToEnumerableRel\n" + "  KuduLimitRel(offset=[5], limit=[20])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL %s])\n" + "      KuduQuery(table=[[kudu, %s]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT1, tableName);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      assertEquals("Unexpected plan ", expectedPlan, plan);

      // even though there is no ORDER BY we force a sort
      rs = conn.createStatement().executeQuery(sql);
      int timestampPartitionIndex = descending ? 2 : 0;
      int rowNum = 5;
      for (int i = 0; i < 3; ++i) {
        // we should get 5 rows from T1, 10 rows from T2 and 5 rows from T3
        int jEnd = i == 1 ? 10 : 5;
        for (int j = 0; j < jEnd; ++j) {
          assertTrue(rs.next());
          validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + (rowNum++), ACCOUNT1);
          // if we are reading the last row from the partition
          if (rowNum == 10) {
            rowNum = 0;
            if (descending) {
              timestampPartitionIndex--;
            } else {
              timestampPartitionIndex++;
            }
          }
        }
      }
      assertFalse(rs.next());
    }
  }

  @Test
  public void testNotHandledFilter() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String sqlFormat = "SELECT * FROM %s WHERE account_sid = '%s' AND phonenumber like '%%0' ";
      String sql = String.format(sqlFormat, tableName, ACCOUNT1);

      // verify that account_sid is pushed down to kudu
      // NOTE: '%%0' is because this string is passed into String.format().
      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduFilterRel(ScanToken 1=[account_sid EQUAL ACCOUNT1], MemoryFilters=[AND(=($0, 'ACCOUNT1'), LIKE($3, '%%0'))])\n"
          + "    KuduQuery(table=[[kudu, %s]])\n";

      String expectedPlan = String.format(expectedPlanFormat, tableName);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);

      // query should return 15 rows with phone numbers ending in 0
      rs = conn.createStatement().executeQuery(sql);
      for (int i = 0; i < 15; ++i) {
        assertTrue(rs.next());
        assertEquals(rs.getString("account_sid"), ACCOUNT1);
        assertEquals(rs.getString("PHONENUMBER"), "512-123-1230");
      }
      assertFalse(rs.next());
    }
  }

  @Test
  public void testSortWithOrg() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String firstBatchSqlFormat = "SELECT * FROM %s " + "WHERE account_sid = '%s' "
              + "ORDER BY account_sid, date_initiated %s, transaction_id " + "LIMIT 6 OFFSET 7";
      String firstBatchSql = String.format(firstBatchSqlFormat, tableName, ACCOUNT1, descending ? "DESC" : "ASC");

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "KuduToEnumerableRel\n"
              + "  KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[%s], "
              + "dir2=[ASC], offset=[7], fetch=[6], groupBySorted=[false])\n"
              + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL %s])\n" + "      KuduQuery(table=[[kudu, %s]])\n";
      String expectedPlan = String.format(expectedPlanFormat, descending ? "DESC" : "ASC", ACCOUNT1, tableName);
      assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);
      rs = conn.createStatement().executeQuery(firstBatchSql);

      if (descending) {
        assertTrue(rs.next());
        validateRow(rs, T3, "TXN7", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T3, "TXN8", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T3, "TXN9", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN0", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN1", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN2", ACCOUNT1);
        assertFalse(rs.next());
      } else {
        assertTrue(rs.next());
        validateRow(rs, T1, "TXN7", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T1, "TXN8", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T1, "TXN9", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN0", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN1", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN2", ACCOUNT1);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testSortWithFilterAndLimitAndOffset() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String firstBatchSqlFormat = "SELECT * FROM %s " + "WHERE account_sid = '%s' "
          + "ORDER BY account_sid, date_initiated %s, transaction_id " + "LIMIT 6 OFFSET 7";
      String firstBatchSql = String.format(firstBatchSqlFormat, tableName, ACCOUNT1, descending ? "DESC" : "ASC");

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[%s], "
          + "dir2=[ASC], offset=[7], fetch=[6], groupBySorted=[false])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL %s])\n" + "      KuduQuery(table=[[kudu, %s]])\n";
      String expectedPlan = String.format(expectedPlanFormat, descending ? "DESC" : "ASC", ACCOUNT1, tableName);
      assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);
      rs = conn.createStatement().executeQuery(firstBatchSql);

      if (descending) {
        assertTrue(rs.next());
        validateRow(rs, T3, "TXN7", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T3, "TXN8", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T3, "TXN9", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN0", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN1", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN2", ACCOUNT1);
        assertFalse(rs.next());
      } else {
        assertTrue(rs.next());
        validateRow(rs, T1, "TXN7", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T1, "TXN8", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T1, "TXN9", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN0", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN1", ACCOUNT1);
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN2", ACCOUNT1);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testPaginationSingleAccount() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      TimestampString lowerBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T1);
      TimestampString upperBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T4);
      String dateInitiatedOrder = descending ? "DESC" : "ASC";
      String firstBatchSqlFormat = "SELECT * FROM %s " + "WHERE account_sid = '%s' "
          + "AND date_initiated >= TIMESTAMP'%s' AND date_initiated < TIMESTAMP'%s' "
          + "ORDER BY date_initiated %s, transaction_id " + "LIMIT 4";
      String firstBatchSql = String.format(firstBatchSqlFormat, tableName, ACCOUNT2, lowerBoundDateInitiated,
          upperBoundDateInitiated, dateInitiatedOrder);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);

      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$1], sort1=[$2], dir0=[%s], dir1=[ASC], fetch=[4], groupBySorted=[false])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL %s, date_initiated GREATER_EQUAL %d, date_initiated LESS %d])\n"
          + "      KuduQuery(table=[[kudu, %s]])\n";
      String expectedPlan = String.format(expectedPlanFormat, dateInitiatedOrder, ACCOUNT2, T1 * 1000, T4 * 1000,
          tableName);
      assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);

      // since there are 30 rows in total we will read 7 batches of four rows,
      // the last batch will have two rows

      // read the first batch of four rows
      int rowNum = 0;
      int timestampPartitionIndex = descending ? 2 : 0;
      rs = conn.createStatement().executeQuery(firstBatchSql);

      for (int i = 0; i < 4; ++i) {
        assertTrue(rs.next());
        validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + (rowNum++), ACCOUNT2);
      }
      // kudu uses nanoseconds
      long prevRowDateInitiatedNanos = rs.getTimestamp(2).getTime() * 1000;
      TimestampString prevRowDateInitiated = TimestampString.fromMillisSinceEpoch(prevRowDateInitiatedNanos / 1000);
      String prevRowTransactionId = rs.getString(3);
      assertFalse(rs.next());

      String nextBatchSqlFormat = "SELECT * FROM %s "
          + "WHERE account_sid = '%s' AND date_initiated >= TIMESTAMP'%s' AND " + "date_initiated < TIMESTAMP'%s' "
          + "AND (date_initiated, transaction_id) > (TIMESTAMP'%s', '%s') "
          + "ORDER BY date_initiated %s, transaction_id " + "LIMIT 4";
      expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$1], sort1=[$2], dir0=[%s], dir1=[ASC], fetch=[4], groupBySorted=[false])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL %s, "
          + "date_initiated GREATER_EQUAL 1000000, date_initiated LESS 4000000, "
          + "date_initiated %s %d], ScanToken 2=[account_sid EQUAL %s, "
          + "date_initiated GREATER_EQUAL 1000000, date_initiated LESS 4000000, "
          + "date_initiated EQUAL %d, transaction_id GREATER %s])\n" + "      KuduQuery(table=[[kudu, %s]])\n";

      // keep reading batches of rows until we have processes rows for all the
      // partitions
      for (int i = 0; i < 7; ++i) {
        // TODO see if we can get bind variables working so that we can use prepared
        // statements
        String nextBatchSql = String.format(nextBatchSqlFormat, tableName, ACCOUNT2, lowerBoundDateInitiated,
            upperBoundDateInitiated, prevRowDateInitiated, prevRowTransactionId, dateInitiatedOrder);

        // verify plan
        rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + nextBatchSql);
        plan = SqlUtil.getExplainPlan(rs);
        expectedPlan = String.format(expectedPlanFormat, dateInitiatedOrder, ACCOUNT2, descending ? "LESS" : "GREATER",
            prevRowDateInitiatedNanos, ACCOUNT2, prevRowDateInitiatedNanos, prevRowTransactionId, tableName);
        assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);

        rs = conn.createStatement().executeQuery(nextBatchSql);
        // the last batch has only two rows
        int jEnd = i == 6 ? 2 : 4;
        for (int j = 0; j < jEnd; ++j) {
          assertTrue("Mismatch in row " + j + " of batch " + i, rs.next());
          validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + rowNum++, ACCOUNT2);
          // if we are reading the last row from the partition
          if (rowNum == 10) {
            rowNum = 0;
            if (descending) {
              timestampPartitionIndex--;
            } else {
              timestampPartitionIndex++;
            }
          }
        }
        prevRowDateInitiatedNanos = rs.getTimestamp(2).getTime() * 1000;
        prevRowDateInitiated = TimestampString.fromMillisSinceEpoch(prevRowDateInitiatedNanos / 1000);
        prevRowTransactionId = rs.getString(3);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testPaginationMultipleAccountsOrderedByTimestamp() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      TimestampString lowerBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T1);
      TimestampString upperBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T4);
      String dateInitiatedOrder = descending ? "DESC" : "ASC";
      String firstBatchSqlFormat = "SELECT * FROM %s WHERE (account_sid = '%s' OR account_sid = "
          + "'%s') AND date_initiated >= TIMESTAMP'%s' AND date_initiated < TIMESTAMP'%s' "
          + "ORDER BY date_initiated %s, transaction_id " + "LIMIT 7";
      String firstBatchSql = String.format(firstBatchSqlFormat, tableName, ACCOUNT1, ACCOUNT2, lowerBoundDateInitiated,
          upperBoundDateInitiated, dateInitiatedOrder);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);

      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$1], sort1=[$2], dir0=[%s], dir1=[ASC], fetch=[7], " + "groupBySorted=[false])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL %s, date_initiated GREATER_EQUAL %d, "
          + "date_initiated LESS %d], ScanToken 2=[account_sid EQUAL %s, date_initiated "
          + "GREATER_EQUAL %d, date_initiated LESS %d])\n" + "      KuduQuery(table=[[kudu, %s]])\n";
      String expectedPlan = String.format(expectedPlanFormat, dateInitiatedOrder, ACCOUNT1, T1 * 1000, T4 * 1000,
          ACCOUNT2, T1 * 1000, T4 * 1000, tableName);
      assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);

      // Since there are 60 rows in total we will read 8 batches of 7 rows,
      // the last batch will have 4 rows. We cannot validate the ACCOUNT_SID since
      // rows can be
      // returned for either account if they have the same timestamp and sid
      // The rows will be returned sorted by timestamp interleaved between the two
      // accounts for eg
      // ACCOUNT1 1969-12-31 16:00:01.001 TXN0
      // ACCOUNT2 1969-12-31 16:00:01.001 TXN0
      // ACCOUNT1 1969-12-31 16:00:01.001 TXN1
      // ACCOUNT2 1969-12-31 16:00:01.001 TXN1

      // read the first batch of 7 rows
      int rowNum = 0;
      int timestampPartitionIndex = descending ? 2 : 0;
      rs = conn.createStatement().executeQuery(firstBatchSql);

      Object[] lastRowAccount1 = null;
      Object[] lastRowAccount2 = null;
      for (int i = 0; i < 7; ++i) {
        assertTrue(rs.next());
        Object[] lastRow = validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + rowNum, null);
        if (i % 2 == 1) {
          rowNum++;
        }
        if (lastRow[0].equals(ACCOUNT1)) {
          lastRowAccount1 = lastRow;
        } else {
          lastRowAccount2 = lastRow;
        }
      }
      assertFalse(rs.next());

      String nextBatchSqlFormat = "SELECT * FROM %s "
          + "WHERE ((account_sid = 'ACCOUNT1' AND (date_initiated, transaction_id) > (TIMESTAMP'%s', '%s'))"
          + " OR (account_sid = 'ACCOUNT2' AND (date_initiated, transaction_id) > (TIMESTAMP'%s', '%s'))) "
          + "AND date_initiated >= TIMESTAMP'%s' AND " + "date_initiated < TIMESTAMP'%s' "
          + "ORDER BY date_initiated %s, transaction_id LIMIT 7";
      expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$1], sort1=[$2], dir0=[%s], dir1=[ASC], fetch=[7], groupBySorted=[false])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL ACCOUNT1, date_initiated %s %d, date_initiated GREATER_EQUAL 1000000, date_initiated LESS 4000000], "
          + "ScanToken 2=[account_sid EQUAL ACCOUNT1, date_initiated EQUAL %d, transaction_id GREATER %s, date_initiated GREATER_EQUAL 1000000, date_initiated LESS 4000000], "
          + "ScanToken 3=[account_sid EQUAL ACCOUNT2, date_initiated %s %d, date_initiated GREATER_EQUAL 1000000, date_initiated LESS 4000000], "
          + "ScanToken 4=[account_sid EQUAL ACCOUNT2, date_initiated EQUAL %d, transaction_id GREATER %s, date_initiated GREATER_EQUAL 1000000, date_initiated LESS 4000000])\n"
          + "      KuduQuery(table=[[kudu, %s]])\n";

      // keep reading batches of rows until we have processes rows for all the
      // partitions
      for (int i = 0; i < 8; ++i) {
        String nextBatchSql = String.format(nextBatchSqlFormat, tableName,
            TimestampString.fromMillisSinceEpoch((long) lastRowAccount1[1]), lastRowAccount1[2],
            TimestampString.fromMillisSinceEpoch((long) lastRowAccount2[1]), lastRowAccount2[2],
            lowerBoundDateInitiated, upperBoundDateInitiated, dateInitiatedOrder);

        // verify plan
        rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + nextBatchSql);
        plan = SqlUtil.getExplainPlan(rs);
        expectedPlan = String.format(expectedPlanFormat, dateInitiatedOrder, descending ? "LESS" : "GREATER",
            (long) lastRowAccount1[1] * 1000, (long) lastRowAccount1[1] * 1000, lastRowAccount1[2],
            descending ? "LESS" : "GREATER", (long) lastRowAccount2[1] * 1000, (long) lastRowAccount2[1] * 1000,
            lastRowAccount2[2], tableName);
        assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);

        rs = conn.createStatement().executeQuery(nextBatchSql);
        // the last batch has only two rows
        int jEnd = i == 7 ? 4 : 7;
        for (int j = 0; j < jEnd; ++j) {
          assertTrue("Mismatch in row " + j + " of batch " + i, rs.next());
          Object[] lastRow = validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + rowNum, null);
          if (((i * 7) + j) % 2 == 0) {
            rowNum++;
          }
          if (lastRow[0].equals(ACCOUNT1)) {
            lastRowAccount1 = lastRow;
          } else {
            lastRowAccount2 = lastRow;
          }
          // if we are reading the last row from the partition
          if (rowNum == 10) {
            rowNum = 0;
            if (descending) {
              timestampPartitionIndex--;
            } else {
              timestampPartitionIndex++;
            }
          }
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testPaginationMultipleAccountsOrderedByAccountSid() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      TimestampString lowerBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T1);
      TimestampString upperBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T4);
      String dateInitiatedOrder = descending ? "DESC" : "ASC";
      String firstBatchSqlFormat = "SELECT * FROM %s WHERE (account_sid = 'ACCOUNT1' OR account_sid = 'ACCOUNT2') AND date_initiated >= TIMESTAMP'%s' AND date_initiated < TIMESTAMP'%s' "
          + "ORDER BY account_sid, date_initiated %s, transaction_id " + "LIMIT 7";
      String firstBatchSql = String.format(firstBatchSqlFormat, tableName, lowerBoundDateInitiated,
          upperBoundDateInitiated, dateInitiatedOrder);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);

      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[%s], dir2=[ASC], "
          + "fetch=[7], groupBySorted=[false])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL %s, date_initiated GREATER_EQUAL %d, "
          + "date_initiated LESS %d], ScanToken 2=[account_sid EQUAL %s, date_initiated "
          + "GREATER_EQUAL %d, date_initiated LESS %d])\n" + "      KuduQuery(table=[[kudu, %s]])\n";
      String expectedPlan = String.format(expectedPlanFormat, dateInitiatedOrder, ACCOUNT1, T1 * 1000, T4 * 1000,
          ACCOUNT2, T1 * 1000, T4 * 1000, tableName);
      assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);

      // Since there are 60 rows in total we will read 8 batches of 7 rows,
      // the last batch will have 4 rows. We cannot validate the ACCOUNT_SID since
      // rows can be
      // returned for either account if they have the same timestamp and sid
      // Rows for ACCOUNT1 will be returned first and then ACCOUNT2

      // read the first batch of 7 rows
      int rowNum = 0;
      int timestampPartitionIndex = descending ? 2 : 0;
      rs = conn.createStatement().executeQuery(firstBatchSql);

      Object[] lastRow = null;
      String expectedAccountSid = ACCOUNT1;
      for (int i = 0; i < 7; ++i) {
        assertTrue(rs.next());
        lastRow = validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + rowNum++, expectedAccountSid);
      }
      assertFalse(rs.next());

      String nextBatchSqlFormat = "SELECT * FROM %s "
          + "WHERE ((account_sid = 'ACCOUNT1' OR account_sid = 'ACCOUNT2') AND (account_sid, "
          + "date_initiated, transaction_id) > ('%s', TIMESTAMP'%s', '%s'))"
          + "AND date_initiated >= TIMESTAMP'%s' AND " + "date_initiated < TIMESTAMP'%s' "
          + "ORDER BY account_sid, date_initiated %s, transaction_id LIMIT 7";

      // keep reading batches of rows until we have processes rows for all the
      // partitions
      for (int i = 0; i < 8; ++i) {
        String nextBatchSql = String.format(nextBatchSqlFormat, tableName, expectedAccountSid,
            TimestampString.fromMillisSinceEpoch((long) lastRow[1]), lastRow[2], lowerBoundDateInitiated,
            upperBoundDateInitiated, dateInitiatedOrder);

        // verify plan
        rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + nextBatchSql);
        plan = SqlUtil.getExplainPlan(rs);
        // The plan is of the form
        // KuduToEnumerableRel
        // KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC],
        // dir2=[ASC], fetch=[7], groupBySorted=[false])
        // KuduFilterRel(
        // ScanToken 1=[account_sid EQUAL ACCOUNT1, account_sid GREATER ACCOUNT1,
        // date_initiated GREATER_EQUAL 1000000, date_initiated LESS 4000000],
        // ScanToken 2=[account_sid EQUAL ACCOUNT1, account_sid EQUAL ACCOUNT1,
        // date_initiated GREATER 1001000, date_initiated GREATER_EQUAL 1000000,
        // date_initiated LESS 4000000],
        // ScanToken 3=[account_sid EQUAL ACCOUNT1, account_sid EQUAL ACCOUNT1,
        // date_initiated EQUAL 1001000, transaction_id GREATER TXN6, date_initiated
        // GREATER_EQUAL 1000000, date_initiated LESS 4000000], ScanToken 4=[account_sid
        // EQUAL ACCOUNT2, account_sid GREATER ACCOUNT1, date_initiated GREATER_EQUAL
        // 1000000, date_initiated LESS 4000000],
        // ScanToken 5=[account_sid EQUAL ACCOUNT2, account_sid EQUAL ACCOUNT1,
        // date_initiated GREATER 1001000, date_initiated GREATER_EQUAL 1000000,
        // date_initiated LESS 4000000],
        // ScanToken 6=[account_sid EQUAL ACCOUNT2, account_sid EQUAL ACCOUNT1,
        // date_initiated EQUAL 1001000, transaction_id GREATER TXN6, date_initiated
        // GREATER_EQUAL 1000000, date_initiated LESS 4000000])
        // KuduQuery(table=[[kudu, TABLE_ASC]])

        // assert that the sort is pushed down
        assertTrue(plan.contains("KuduSortRel"));

        rs = conn.createStatement().executeQuery(nextBatchSql);
        // the last batch has only two rows
        int jEnd = i == 7 ? 4 : 7;
        for (int j = 0; j < jEnd; ++j) {
          assertTrue("Mismatch in row " + j + " of batch " + i, rs.next());
          lastRow = validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + rowNum++,
              expectedAccountSid);
          // if we are reading the last row from the partition
          if (rowNum == 10) {
            rowNum = 0;
            if (descending) {
              timestampPartitionIndex--;
              // if we read all rows from ACCOUNT1
              if (timestampPartitionIndex == -1) {
                timestampPartitionIndex = 2;
                expectedAccountSid = ACCOUNT2;
              }
            } else {
              timestampPartitionIndex++;
              // if we read all rows from ACCOUNT1
              if (timestampPartitionIndex == 3) {
                timestampPartitionIndex = 0;
                expectedAccountSid = ACCOUNT2;
              }
            }
          }
        }
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testPaginationMultipleAccountsOrderedByAccountSidUsingIN() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      TimestampString lowerBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T1);
      TimestampString upperBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T4);
      String dateInitiatedOrder = descending ? "DESC" : "ASC";
      String hint = "/*+ USE_OR_CLAUSE */";
      String firstBatchSqlFormat = "SELECT %s * FROM %s WHERE account_sid IN ('ACCOUNT1','ACCOUNT2') AND date_initiated >= TIMESTAMP'%s' AND date_initiated < TIMESTAMP'%s' "
              + "ORDER BY account_sid, date_initiated %s, transaction_id " + "LIMIT 7";

      String firstBatchSql = String.format(firstBatchSqlFormat, hint, tableName, lowerBoundDateInitiated,
              upperBoundDateInitiated, dateInitiatedOrder);
     // String sql = String.format(firstBatchSql, hint);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);

      String expectedPlanFormat = "KuduToEnumerableRel\n"
              + "  KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[%s], dir2=[ASC], "
              + "fetch=[7], groupBySorted=[false])\n"
              + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL %s, date_initiated GREATER_EQUAL %d, "
              + "date_initiated LESS %d], ScanToken 2=[account_sid EQUAL %s, date_initiated "
              + "GREATER_EQUAL %d, date_initiated LESS %d])\n" + "      KuduQuery(table=[[kudu, %s]])\n";
      String expectedPlan = String.format(expectedPlanFormat, dateInitiatedOrder, ACCOUNT1, T1 * 1000, T4 * 1000,
              ACCOUNT2, T1 * 1000, T4 * 1000, tableName);
      assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);

      // Since there are 60 rows in total we will read 8 batches of 7 rows,
      // the last batch will have 4 rows. We cannot validate the ACCOUNT_SID since
      // rows can be
      // returned for either account if they have the same timestamp and sid
      // Rows for ACCOUNT1 will be returned first and then ACCOUNT2

      // read the first batch of 7 rows
      int rowNum = 0;
      int timestampPartitionIndex = descending ? 2 : 0;
      rs = conn.createStatement().executeQuery(firstBatchSql);

      Object[] lastRow = null;
      String expectedAccountSid = ACCOUNT1;
      for (int i = 0; i < 7; ++i) {
        assertTrue(rs.next());
        lastRow = validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + rowNum++, expectedAccountSid);
      }
      assertFalse(rs.next());

      String nextBatchSqlFormat = "SELECT * FROM %s "
              + "WHERE ((account_sid = 'ACCOUNT1' OR account_sid = 'ACCOUNT2') AND (account_sid, "
              + "date_initiated, transaction_id) > ('%s', TIMESTAMP'%s', '%s'))"
              + "AND date_initiated >= TIMESTAMP'%s' AND " + "date_initiated < TIMESTAMP'%s' "
              + "ORDER BY account_sid, date_initiated %s, transaction_id LIMIT 7";

//      // keep reading batches of rows until we have processes rows for all the
//      // partitions
//      for (int i = 0; i < 8; ++i) {
//        String nextBatchSql = String.format(nextBatchSqlFormat, tableName, expectedAccountSid,
//                TimestampString.fromMillisSinceEpoch((long) lastRow[1]), lastRow[2], lowerBoundDateInitiated,
//                upperBoundDateInitiated, dateInitiatedOrder);
//
//        // verify plan
//        rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + nextBatchSql);
//        plan = SqlUtil.getExplainPlan(rs);
//        // The plan is of the form
//        // KuduToEnumerableRel
//        // KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC],
//        // dir2=[ASC], fetch=[7], groupBySorted=[false])
//        // KuduFilterRel(
//        // ScanToken 1=[account_sid EQUAL ACCOUNT1, account_sid GREATER ACCOUNT1,
//        // date_initiated GREATER_EQUAL 1000000, date_initiated LESS 4000000],
//        // ScanToken 2=[account_sid EQUAL ACCOUNT1, account_sid EQUAL ACCOUNT1,
//        // date_initiated GREATER 1001000, date_initiated GREATER_EQUAL 1000000,
//        // date_initiated LESS 4000000],
//        // ScanToken 3=[account_sid EQUAL ACCOUNT1, account_sid EQUAL ACCOUNT1,
//        // date_initiated EQUAL 1001000, transaction_id GREATER TXN6, date_initiated
//        // GREATER_EQUAL 1000000, date_initiated LESS 4000000], ScanToken 4=[account_sid
//        // EQUAL ACCOUNT2, account_sid GREATER ACCOUNT1, date_initiated GREATER_EQUAL
//        // 1000000, date_initiated LESS 4000000],
//        // ScanToken 5=[account_sid EQUAL ACCOUNT2, account_sid EQUAL ACCOUNT1,
//        // date_initiated GREATER 1001000, date_initiated GREATER_EQUAL 1000000,
//        // date_initiated LESS 4000000],
//        // ScanToken 6=[account_sid EQUAL ACCOUNT2, account_sid EQUAL ACCOUNT1,
//        // date_initiated EQUAL 1001000, transaction_id GREATER TXN6, date_initiated
//        // GREATER_EQUAL 1000000, date_initiated LESS 4000000])
//        // KuduQuery(table=[[kudu, TABLE_ASC]])
//
//        // assert that the sort is pushed down
//        assertTrue(plan.contains("KuduSortRel"));
//
//        rs = conn.createStatement().executeQuery(nextBatchSql);
//        // the last batch has only two rows
//        int jEnd = i == 7 ? 4 : 7;
//        for (int j = 0; j < jEnd; ++j) {
//          assertTrue("Mismatch in row " + j + " of batch " + i, rs.next());
//          lastRow = validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + rowNum++,
//                  expectedAccountSid);
//          // if we are reading the last row from the partition
//          if (rowNum == 10) {
//            rowNum = 0;
//            if (descending) {
//              timestampPartitionIndex--;
//              // if we read all rows from ACCOUNT1
//              if (timestampPartitionIndex == -1) {
//                timestampPartitionIndex = 2;
//                expectedAccountSid = ACCOUNT2;
//              }
//            } else {
//              timestampPartitionIndex++;
//              // if we read all rows from ACCOUNT1
//              if (timestampPartitionIndex == 3) {
//                timestampPartitionIndex = 0;
//                expectedAccountSid = ACCOUNT2;
//              }
//            }
//          }
//        }
//        assertFalse(rs.next());
//      }
    }
  }

}
