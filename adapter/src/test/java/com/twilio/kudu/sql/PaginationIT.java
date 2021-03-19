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
  public static final long[] TIMESTAMP_PARTITIONS = new long[] { T1, T2, T3 };
  public static final int NUM_ROWS_PER_PARTITION = 10;

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

  public static void validateRow(ResultSet rs, long expectedTimestamp, String expectedTransactionId)
      throws SQLException {
    assertEquals("Mismatched usage account sid", ACCOUNT1, rs.getString(ACCOUNT_SID));
    assertEquals("Mismatched date initiated", expectedTimestamp + 1,
        rs.getTimestamp(DATE_INITIATED).toInstant().toEpochMilli());
    assertEquals("Mismatched transaction id", expectedTransactionId, rs.getString(TRANSACTION_ID));
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
          validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + (rowNum++));
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
        validateRow(rs, T3, "TXN7");
        assertTrue(rs.next());
        validateRow(rs, T3, "TXN8");
        assertTrue(rs.next());
        validateRow(rs, T3, "TXN9");
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN0");
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN1");
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN2");
        assertFalse(rs.next());
      } else {
        assertTrue(rs.next());
        validateRow(rs, T1, "TXN7");
        assertTrue(rs.next());
        validateRow(rs, T1, "TXN8");
        assertTrue(rs.next());
        validateRow(rs, T1, "TXN9");
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN0");
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN1");
        assertTrue(rs.next());
        validateRow(rs, T2, "TXN2");
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testQueryMore() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      TimestampString lowerBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T1);
      TimestampString upperBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T4);
      String dateInitiatedOrder = descending ? "DESC" : "ASC";
      String firstBatchSqlFormat = "SELECT * FROM %s " + "WHERE account_sid = '%s' "
          + "AND date_initiated >= TIMESTAMP'%s' AND date_initiated < TIMESTAMP'%s' "
          + "ORDER BY date_initiated %s, transaction_id " + "LIMIT 4";
      String firstBatchSql = String.format(firstBatchSqlFormat, tableName, ACCOUNT1, lowerBoundDateInitiated,
          upperBoundDateInitiated, dateInitiatedOrder);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);

      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$1], sort1=[$2], dir0=[%s], dir1=[ASC], fetch=[4], groupBySorted=[false])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL %s, date_initiated GREATER_EQUAL %d, date_initiated LESS %d])\n"
          + "      KuduQuery(table=[[kudu, %s]])\n";
      String expectedPlan = String.format(expectedPlanFormat, dateInitiatedOrder, ACCOUNT1, T1 * 1000, T4 * 1000,
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
        validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + (rowNum++));
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
        String nextBatchSql = String.format(nextBatchSqlFormat, tableName, ACCOUNT1, lowerBoundDateInitiated,
            upperBoundDateInitiated, prevRowDateInitiated, prevRowTransactionId, dateInitiatedOrder);

        // verify plan
        rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + nextBatchSql);
        plan = SqlUtil.getExplainPlan(rs);
        expectedPlan = String.format(expectedPlanFormat, dateInitiatedOrder, ACCOUNT1, descending ? "LESS" : "GREATER",
            prevRowDateInitiatedNanos, ACCOUNT1, prevRowDateInitiatedNanos, prevRowTransactionId, tableName);
        assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);

        rs = conn.createStatement().executeQuery(nextBatchSql);
        // the last batch has only two rows
        int jEnd = i == 6 ? 2 : 4;
        for (int j = 0; j < jEnd; ++j) {
          assertTrue("Mismatch in row " + j + " of batch " + i, rs.next());
          validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + rowNum++);
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

}
