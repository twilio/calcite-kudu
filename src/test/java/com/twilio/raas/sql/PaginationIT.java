package com.twilio.raas.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.TimestampString;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PaginationIT {

    public static final String TABLE_NAME = "ReportCenter.UsageReportTransactions";
    public static final String USAGE_ACCOUNT_SID = "usage_account_sid";
    public static final String DATE_INITIATED = "date_initiated";
    public static final String TRANSACTION_ID = "transaction_id";
    public static final String PHONENUMBER = "phonenumber";
    public static final String AMOUNT = "amount";
    public static final String BILLABLE_ITEM = "billable_item";

    @ClassRule
    public static KuduTestHarness testHarness = new KuduTestHarness();
    public static JDBCQueryRunner runner;
    public static KuduTable kuduTable;

    public static final String ACCOUNT1 = "AC3b1ebbfc4cd2fc2485ed634788370001";
    public static final String ACCOUNT2 = "AC3b1ebbfc4cd2fc2485ed634788370002";
    public static final String PHONENUMBER_BI = "BIf4811c26b8928ca0e66cf318618bd403";
    public static final long T1 = 1000;
    public static final long T2 = 2000;
    public static final long T3 = 3000;
    public static final long T4 = 4000;
    public static final String[] ACCOUNTS = new String[] {ACCOUNT1, ACCOUNT2};
    public static final long[] TIMESTAMP_PARTITIONS = new long[] {T1, T2, T3};
    public static final int NUM_ROWS_PER_PARTITION = 10;

    @BeforeClass
    public static void setup() throws Exception {
        createUsageReportTransactionTable(testHarness.getClient());
        runner = new JDBCQueryRunner(testHarness.getMasterAddressesAsString(), 1);
    }

    /**
     * Creates a table with 3 hash partitions and 3 date range partitions. Loads 10 rows per date
     * parition per (for a total of 30 rows) for two accounts.
     */
    public static void createUsageReportTransactionTable(KuduClient kuduClient) throws Exception {
        // create the table
        ColumnTypeAttributes scaleAndPrecision = new ColumnTypeAttributes.ColumnTypeAttributesBuilder().scale(22).precision(6).build();
        final List<ColumnSchema> columns = Arrays.asList(
                new ColumnSchema.ColumnSchemaBuilder(USAGE_ACCOUNT_SID, Type.STRING).key(true).build(),
                new ColumnSchema.ColumnSchemaBuilder(DATE_INITIATED, Type.UNIXTIME_MICROS).key(true).build(),
                new ColumnSchema.ColumnSchemaBuilder(TRANSACTION_ID, Type.STRING).key(true).build(),
                new ColumnSchema.ColumnSchemaBuilder(BILLABLE_ITEM, Type.STRING).build(),
                new ColumnSchema.ColumnSchemaBuilder(PHONENUMBER, Type.STRING).build(),
                new ColumnSchema.ColumnSchemaBuilder(AMOUNT, Type.DECIMAL).typeAttributes(scaleAndPrecision).build()
        );

        Schema schema = new Schema(columns);
        PartialRow row1 = schema.newPartialRow();
        row1.addTimestamp(DATE_INITIATED, new Timestamp(T1));
        PartialRow row2 = schema.newPartialRow();
        row2.addTimestamp(DATE_INITIATED, new Timestamp(T2));
        PartialRow row3 = schema.newPartialRow();
        row3.addTimestamp(DATE_INITIATED, new Timestamp(T3));
        PartialRow row4 = schema.newPartialRow();
        row3.addTimestamp(DATE_INITIATED, new Timestamp(T4));

        // create a table with 3 hash partitions and 3 range partitions for a total of 9 tablets
        CreateTableOptions tableBuilder = new CreateTableOptions();
        tableBuilder.addHashPartitions(Arrays.asList(USAGE_ACCOUNT_SID), 3)
                .setRangePartitionColumns(ImmutableList.of(DATE_INITIATED))
                .addRangePartition(row1, row2) //[1000,2000)
                .addRangePartition(row2, row3) //[2000,3000)
                .addRangePartition(row3, row4) //[3000,4000)
                .setNumReplicas(1);
        kuduClient.createTable(TABLE_NAME, schema, tableBuilder);
        kuduTable = kuduClient.openTable(TABLE_NAME);

        KuduSession insertSession = kuduClient.newSession();
        // insert 10 rows for each range partition
        for (String account : ACCOUNTS) {
            for (long timestampPartition : TIMESTAMP_PARTITIONS) {
                int counter = 0;
                for (int i = 0; i < NUM_ROWS_PER_PARTITION; ++i) {
                    insertRow(insertSession, account, new Timestamp(timestampPartition), counter++);
                }
            }
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testHarness.getClient().deleteTable(TABLE_NAME);
        runner.close();
    }

    private static void insertRow(KuduSession insertSession, String usageAccountSid,
                           Timestamp dateInitiated, int id) throws Exception {
        Upsert upsert = kuduTable.newUpsert();
        PartialRow row = upsert.getRow();
        row.addString(USAGE_ACCOUNT_SID, usageAccountSid);
        row.addTimestamp(DATE_INITIATED, dateInitiated);
        row.addString(TRANSACTION_ID, "TXN" + (id));
        row.addString(PHONENUMBER, "512-123-123"+(id%2));
        row.addString(BILLABLE_ITEM, PHONENUMBER_BI);
        row.addDecimal(AMOUNT, BigDecimal.valueOf(-100l, 22));
        insertSession.apply(upsert);
    }

    public static void validateRow(ResultSet rs, long expectedTimestamp,
                                String expectedTransactionId) throws SQLException {
        assertEquals("Mismatched usage account sid", ACCOUNT1,
                rs.getString(USAGE_ACCOUNT_SID));
        assertEquals("Mismatched date initiated", expectedTimestamp,
                rs.getTimestamp(DATE_INITIATED).toInstant().toEpochMilli());
        assertEquals("Mismatched transaction id", expectedTransactionId,
                rs.getString(TRANSACTION_ID));
    }

    @Test
    public void testLimit() throws Exception {
        String url = String.format(JDBCQueryRunner.CALCITE_MODEL_TEMPLATE, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {
            String sql = "SELECT * FROM kudu.\"ReportCenter.UsageReportTransactions\" LIMIT 3";
            String expectedPlan = "KuduToEnumerableRel\n" +
                    "  KuduLimitRel(limit=[3])\n" +
                    "    KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
            String plan = SqlUtil.getExplainPlan(rs);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            // since the rows are not ordered just assert that we get the expected number of rows
            rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
    }

    @Test
    public void testFilterWithLimitAndOffset() throws Exception {
        String url = String.format(JDBCQueryRunner.CALCITE_MODEL_TEMPLATE, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {
            // this query will return rows in an unpredictable order
            String sqlFormat = "SELECT * FROM kudu.\"ReportCenter.UsageReportTransactions\" "
                    + "WHERE usage_account_sid = '%s' "
                    + "LIMIT 20 OFFSET 5";
            String sql = String.format(sqlFormat, ACCOUNT1);

            // verify plan
            String expectedPlanFormat = "KuduToEnumerableRel\n" +
                    "  KuduLimitRel(offset=[5], limit=[20])\n" +
                    "    KuduFilterRel(ScanToken 1=[usage_account_sid EQUAL %s])\n" +
                    "      KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";
            String expectedPlan = String.format(expectedPlanFormat, ACCOUNT1);
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
            String plan = SqlUtil.getExplainPlan(rs);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            // even though there is no ORDER BY we force a sort
            rs = conn.createStatement().executeQuery(sql);
            int timestampPartitionIndex = 0;
            int rowNum = 5;
            for (int i=0; i<3; ++i) {
                // we should get 5 rows from T1, 10 rows from T2 and 5 rows from T3
                int jEnd = i==1? 10 : 5;
                for (int j = 0; j < jEnd; ++j) {
                    assertTrue(rs.next());
                    validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + (rowNum++));
                    // if we are reading the last row from the partition
                    if (rowNum == 10) {
                        rowNum = 0;
                        timestampPartitionIndex++;
                    }
                }
            }
            assertFalse(rs.next());
        }
    }

    @Test
    public void testNotHandledFilter() throws Exception {
        String url = String.format(JDBCQueryRunner.CALCITE_MODEL_TEMPLATE, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {
            String sqlFormat = "SELECT * FROM kudu.\"ReportCenter.UsageReportTransactions\" "
                    + "WHERE usage_account_sid = '%s' AND phonenumber like '%%0' ";
            String sql = String.format(sqlFormat, ACCOUNT1);

            // verify that usage_account_sid is pushed down to kudu
            String expectedPlanFormat = "EnumerableCalc(expr#0..5=[{inputs}], " +
                    "expr#6=['%s':VARCHAR], expr#7=[=($t0, $t6)]," +
                    " expr#8=['%%0'], expr#9=[LIKE($t4, $t8)], expr#10=[AND($t7, $t9)], proj#0." +
                    ".5=[{exprs}], $condition=[$t10])\n" +
                    "  KuduToEnumerableRel\n" +
                    "    KuduFilterRel(ScanToken 1=[usage_account_sid EQUAL %s])\n" +
                    "      KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";
            String expectedPlan = String.format(expectedPlanFormat, ACCOUNT1, ACCOUNT1);
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
            String plan = SqlUtil.getExplainPlan(rs);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            // query should return 15 rows with phone numbers ending in 0
            rs = conn.createStatement().executeQuery(sql);
            for (int i=0; i<15; ++i) {
                assertTrue(rs.next());
                assertEquals(rs.getString("USAGE_ACCOUNT_SID"), ACCOUNT1);
                assertEquals(rs.getString("PHONENUMBER"), "512-123-1230");
            }
            assertFalse(rs.next());
        }
    }


    @Test
    public void testSortWithFilterAndLimitAndOffset() throws Exception {
        String url = String.format(JDBCQueryRunner.CALCITE_MODEL_TEMPLATE, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {
            String firstBatchSqlFormat = "SELECT * FROM kudu.\"ReportCenter" +
                    ".UsageReportTransactions\" "
                    + "WHERE usage_account_sid = '%s' "
                    + "ORDER BY usage_account_sid, date_initiated, transaction_id "
                    + "LIMIT 6 OFFSET 7";
            String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT1);

            // verify plan
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
            String plan = SqlUtil.getExplainPlan(rs);
            String expectedPlanFormat = "KuduToEnumerableRel\n" +
                    "  KuduLimitRel(offset=[7], limit=[6])\n" +
                    "    KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], " +
                    "dir2=[ASC], groupByLimited=[false])\n" +
                    "      KuduFilterRel(ScanToken 1=[usage_account_sid EQUAL %s])\n" +
                    "        KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";
            String expectedPlan = String.format(expectedPlanFormat, ACCOUNT1);
            assertEquals("Unexpected plan ", expectedPlan, plan);
            rs = conn.createStatement().executeQuery(firstBatchSql);

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

    @Test
    public void testQueryMore() throws Exception {
        String url = String.format(JDBCQueryRunner.CALCITE_MODEL_TEMPLATE, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {
            TimestampString lowerBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T1);
            TimestampString upperBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T4);
            String firstBatchSqlFormat = "SELECT * FROM kudu.\"ReportCenter.UsageReportTransactions\" "
                    + "WHERE usage_account_sid = '%s' "
                    + "AND date_initiated >= TIMESTAMP'%s' AND date_initiated < TIMESTAMP'%s' "
                    + "ORDER BY date_initiated, transaction_id "
                    + "LIMIT 4";
            String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT1,
                    lowerBoundDateInitiated, upperBoundDateInitiated);
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
            String plan = SqlUtil.getExplainPlan(rs);

            String expectedPlanFormat = "KuduToEnumerableRel\n" +
                    "  KuduLimitRel(limit=[4])\n" +
                    "    KuduSortRel(sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[ASC], " +
                    "groupByLimited=[false])\n" +
                    "      KuduFilterRel(ScanToken 1=[usage_account_sid EQUAL %s, " +
                    "date_initiated GREATER_EQUAL %d, date_initiated LESS %d])\n" +
                    "        KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";
            String expectedPlan = String.format(expectedPlanFormat, ACCOUNT1, T1*1000, T4*1000);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            // since there are 30 rows in total we will read 7 batches of four rows,
            // the last batch will have two rows

            // read the first batch of four rows
            int rowNum = 0;
            int timestampPartitionIndex = 0;
            rs = conn.createStatement().executeQuery(firstBatchSql);
            for (int i=0; i<4; ++i) {
                assertTrue(rs.next());
                validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + (rowNum++));
            }
            // kudu uses nanoseconds
            long prevRowDateInitiatedNanos = rs.getTimestamp(2).getTime()*1000;
            TimestampString prevRowDateInitiated =
                    TimestampString.fromMillisSinceEpoch(prevRowDateInitiatedNanos/1000);
            String prevRowTransactionId = rs.getString(3);
            assertFalse(rs.next());

            String nextBatchSqlFormat = "SELECT * FROM kudu.\"ReportCenter" +
                    ".UsageReportTransactions\" "
                    + "WHERE usage_account_sid = '%s' AND date_initiated >= TIMESTAMP'%s' AND " +
                    "date_initiated < TIMESTAMP'%s' "
                    + "AND (date_initiated, transaction_id) > (TIMESTAMP'%s', '%s') "
                    + "ORDER BY date_initiated, transaction_id "
                    + "LIMIT 4";
            expectedPlanFormat = "KuduToEnumerableRel\n" +
                    "  KuduLimitRel(limit=[4])\n" +
                    "    KuduSortRel(sort0=[$1], sort1=[$2], dir0=[ASC], dir1=[ASC], " +
                    "groupByLimited=[false])\n" +
                    "      KuduFilterRel(ScanToken 1=[usage_account_sid EQUAL %s, " +
                    "date_initiated GREATER_EQUAL 1000000, date_initiated LESS 4000000, " +
                    "date_initiated GREATER %d], ScanToken 2=[usage_account_sid EQUAL %s, " +
                    "date_initiated GREATER_EQUAL 1000000, date_initiated LESS 4000000, " +
                    "date_initiated EQUAL %d, transaction_id GREATER %s])\n" +
                    "        KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";

            // keep reading batches of rows until we have processes rows for all the partitions
            for (int i=0; i<7; ++i) {
                // TODO see if we can get bind variables working so that we can use prepared statements
                String nextBatchSql = String.format(nextBatchSqlFormat, ACCOUNT1,
                        lowerBoundDateInitiated, upperBoundDateInitiated, prevRowDateInitiated,
                        prevRowTransactionId);

                // verify plan
                rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + nextBatchSql);
                plan = SqlUtil.getExplainPlan(rs);
                expectedPlan = String.format(expectedPlanFormat, ACCOUNT1,
                        prevRowDateInitiatedNanos, ACCOUNT1, prevRowDateInitiatedNanos,
                        prevRowTransactionId);
                assertEquals("Unexpected plan ", expectedPlan, plan);

                rs = conn.createStatement().executeQuery(nextBatchSql);
                // the last batch has only two rows
                int jEnd = i==6 ? 2 : 4;
                for (int j=0; j<jEnd; ++j) {
                    assertTrue(rs.next());
                    validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN" + (rowNum++));
                    // if we are reading the last row from the partition
                    if (rowNum==10) {
                        rowNum = 0;
                        timestampPartitionIndex++;
                    }
                }
                prevRowDateInitiatedNanos = rs.getTimestamp(2).getTime()*1000;
                prevRowDateInitiated =
                        TimestampString.fromMillisSinceEpoch(prevRowDateInitiatedNanos/1000);
                prevRowTransactionId = rs.getString(3);
                assertFalse(rs.next());
            }
        }
    }

}
