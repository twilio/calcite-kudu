package com.twilio.raas.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.util.TimestampString;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

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

    @ClassRule
    public static KuduTestHarness testHarness = new KuduTestHarness();
    public static JDBCQueryRunner runner;
    public static KuduTable kuduTable;

    private static final String ACCOUNT1 = "AC1234567";
    private static final String ACCOUNT2 = "AC9876543";
    private static final int T1 = 1000;
    private static final int T2 = 2000;
    private static final int T3 = 3000;
    private static final int T4 = 4000;
    private static final String[] ACCOUNTS = new String[] {ACCOUNT1, ACCOUNT2};
    private static final int[] TIMESTAMP_PARTITIONS = new int[] {T1, T2, T3};
    public static final int NUM_ROWS_PER_PARTITION = 10;

    @BeforeClass
    public static void setup() throws Exception {
        // create the table
        final List<ColumnSchema> columns = Arrays.asList(
                new ColumnSchema.ColumnSchemaBuilder(USAGE_ACCOUNT_SID, Type.STRING).key(true).build(),
                new ColumnSchema.ColumnSchemaBuilder(DATE_INITIATED, Type.UNIXTIME_MICROS).key(true).build(),
                new ColumnSchema.ColumnSchemaBuilder(TRANSACTION_ID, Type.STRING).key(true).build(),
                new ColumnSchema.ColumnSchemaBuilder(PHONENUMBER, Type.STRING).nullable(true).build(),
                new ColumnSchema.ColumnSchemaBuilder(AMOUNT, Type.DECIMAL).nullable(true)
                        .typeAttributes(
                                new ColumnTypeAttributes.ColumnTypeAttributesBuilder().scale(22).precision(6).build())
                        .build()
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
        testHarness.getClient().createTable(TABLE_NAME, schema, tableBuilder);
        kuduTable = testHarness.getClient().openTable(TABLE_NAME);
        runner = new JDBCQueryRunner(testHarness.getMasterAddressesAsString(), 1);

        AsyncKuduSession insertSession = testHarness.getAsyncClient().newSession();
        // insert 10 rows for each range partition
        for (String account : ACCOUNTS) {
            for (int timestampPartition : TIMESTAMP_PARTITIONS) {
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

    private static void insertRow(AsyncKuduSession insertSession, String usageAccountSid,
                           Timestamp dateInitiated, int id) throws Exception {
        Upsert upsert = kuduTable.newUpsert();
        PartialRow row = upsert.getRow();
        row.addString(USAGE_ACCOUNT_SID, usageAccountSid);
        row.addTimestamp(DATE_INITIATED, dateInitiated);
        row.addString(TRANSACTION_ID, "TXN" + (id));
        row.addString(PHONENUMBER, "512-123-123"+(id%2));
        insertSession.apply(upsert).join();
    }

    private void validateRow(ResultSet rs, int expectedTimestamp, String expectedTransactionId) throws SQLException {
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
            String plan = TestUtil.getExplainPlan(rs);
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
            String sql = "SELECT * FROM kudu.\"ReportCenter.UsageReportTransactions\" "
                    + "WHERE usage_account_sid = 'AC1234567' "
                    + "LIMIT 3 OFFSET 4";

            // verify plan
            String expectedPlan = "KuduToEnumerableRel\n" +
                    "  KuduLimitRel(offset=[4], limit=[3])\n" +
                    "    KuduFilterRel(Scan 1=[usage_account_sid EQUAL AC1234567])\n" +
                    "      KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
            String plan = TestUtil.getExplainPlan(rs);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            // since the rows are not ordered just assert that the usage_account_sid is what is
            // expected
            rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertEquals(rs.getString("USAGE_ACCOUNT_SID"), "AC1234567");
            assertTrue(rs.next());
            assertEquals(rs.getString("USAGE_ACCOUNT_SID"), "AC1234567");
            assertTrue(rs.next());
            assertEquals(rs.getString("USAGE_ACCOUNT_SID"), "AC1234567");
            assertFalse(rs.next());
        }
    }

    @Test
    public void testNotHandledFilter() throws Exception {
        String url = String.format(JDBCQueryRunner.CALCITE_MODEL_TEMPLATE, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {
            String sql = "SELECT * FROM kudu.\"ReportCenter.UsageReportTransactions\" "
                    + "WHERE usage_account_sid = 'AC1234567' AND phonenumber like '%0' ";

            // verify that usage_account_sid is pushed down to kudu
            String expectedPlan = "EnumerableCalc(expr#0..4=[{inputs}], " +
                    "expr#5=['AC1234567':VARCHAR], expr#6=[=($t0, $t5)], expr#7=['%0'], " +
                    "expr#8=[LIKE($t3, $t7)], expr#9=[AND($t6, $t8)], proj#0..4=[{exprs}], " +
                    "$condition=[$t9])\n" +
                    "  KuduToEnumerableRel\n" +
                    "    KuduFilterRel(Scan 1=[usage_account_sid EQUAL AC1234567])\n" +
                    "      KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
            String plan = TestUtil.getExplainPlan(rs);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            // query should return 15 rows with phone numbers ending in 0
            rs = conn.createStatement().executeQuery(sql);
            for (int i=0; i<15; ++i) {
                assertTrue(rs.next());
                assertEquals(rs.getString("USAGE_ACCOUNT_SID"), "AC1234567");
                assertEquals(rs.getString("PHONENUMBER"), "512-123-1230");
            }
            assertFalse(rs.next());
        }
    }


    @Test
    public void testSortWithFilterAndLimitAndOffset() throws Exception {
        String url = String.format(JDBCQueryRunner.CALCITE_MODEL_TEMPLATE, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {
            String firstBatchSql = "SELECT * FROM kudu.\"ReportCenter" + ".UsageReportTransactions\" "
                    + "WHERE usage_account_sid = 'AC1234567' "
                    + "ORDER BY usage_account_sid, date_initiated, transaction_id "
                    + "LIMIT 6 OFFSET 7";

            // verify plan
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
            String plan = TestUtil.getExplainPlan(rs);
                String expectedPlan = "KuduToEnumerableRel\n" +
                        "  KuduLimitRel(offset=[7], limit=[6])\n" +
                        "    KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC])\n" +
                        "      KuduFilterRel(Scan 1=[usage_account_sid EQUAL AC1234567])\n" +
                        "        KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";
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
            String firstBatchSql = "SELECT * FROM kudu.\"ReportCenter" + ".UsageReportTransactions\" "
                    + "WHERE usage_account_sid = 'AC1234567' "
                    + "ORDER BY usage_account_sid, date_initiated, transaction_id "
                    + "LIMIT 4";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
            String plan = TestUtil.getExplainPlan(rs);

            String expectedPlan = "KuduToEnumerableRel\n" +
                    "  KuduLimitRel(limit=[4])\n" +
                    "    KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC])\n" +
                    "      KuduFilterRel(Scan 1=[usage_account_sid EQUAL AC1234567])\n" +
                    "        KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";
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

            String usageAccountId = "AC1234567";
            TimestampString lowerBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T1);
            TimestampString upperBoundDateInitiated = TimestampString.fromMillisSinceEpoch(T4);
            String nextBatchSqlFormat = "SELECT * FROM kudu.\"ReportCenter" +
                    ".UsageReportTransactions\" "
                    + "WHERE usage_account_sid = '%s' AND date_initiated >= TIMESTAMP'%s' AND " +
                    "date_initiated < TIMESTAMP'%s' "
                    + "AND (date_initiated, transaction_id) > (TIMESTAMP'%s', '%s') "
                    + "ORDER BY usage_account_sid, date_initiated, transaction_id "
                    + "LIMIT 4";
            String expectedPlanFormat = "KuduToEnumerableRel\n" +
                    "  KuduLimitRel(limit=[4])\n" +
                    "    KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC])\n" +
                    "      KuduFilterRel(Scan 1=[usage_account_sid EQUAL AC1234567 , " +
                    "date_initiated GREATER_EQUAL 1000000 , date_initiated LESS 4000000 , " +
                    "date_initiated GREATER %s], Scan 2=[usage_account_sid EQUAL AC1234567 , " +
                    "date_initiated GREATER_EQUAL 1000000 , date_initiated LESS 4000000 , " +
                    "date_initiated EQUAL %s , transaction_id GREATER %s])\n" +
                    "        KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";

            // keep reading batches of rows until we have processes rows for all the partitions
            for (int i=0; i<6; ++i) {
                // TODO see if we can get bind variables working so that we can use prepared statements
                String nextBatchSql = String.format(nextBatchSqlFormat, usageAccountId,
                        lowerBoundDateInitiated, upperBoundDateInitiated, prevRowDateInitiated,
                        prevRowTransactionId);

                // verify plan
                rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + nextBatchSql);
                plan = TestUtil.getExplainPlan(rs);
                expectedPlan = String.format(expectedPlanFormat, prevRowDateInitiatedNanos,
                        prevRowDateInitiatedNanos, prevRowTransactionId);
                assertEquals("Unexpected plan ", expectedPlan, plan);

                rs = conn.createStatement().executeQuery(nextBatchSql);
                for (int j=0; j<4; ++j) {
                    System.out.println("i " + i + " j " + j);
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

            String lastBatchSql = String.format(nextBatchSqlFormat, usageAccountId,
                    lowerBoundDateInitiated, upperBoundDateInitiated, prevRowDateInitiated,
                    prevRowTransactionId);

            // verify plan
            rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + lastBatchSql);
            plan = TestUtil.getExplainPlan(rs);
            expectedPlan = String.format(expectedPlanFormat, prevRowDateInitiatedNanos,
                    prevRowDateInitiatedNanos, prevRowTransactionId);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            rs = conn.createStatement().executeQuery(lastBatchSql);
            assertTrue(rs.next());
            validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN"+(rowNum++));
            assertTrue(rs.next());
            validateRow(rs, TIMESTAMP_PARTITIONS[timestampPartitionIndex], "TXN"+(rowNum++));
            assertFalse(rs.next());
        }
    }

}
