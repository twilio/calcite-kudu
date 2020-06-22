package com.twilio.raas.sql;

import com.google.common.collect.ImmutableMap;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.Schema;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.test.KuduTestHarness;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Timestamp;
import org.junit.runners.JUnit4;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduSession;

import java.util.List;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.AfterClass;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public final class JDBCQueryIT {
    private static final Logger logger = LoggerFactory.getLogger(JDBCQueryIT.class);

    public static String FIRST_SID = "SM1";
    public static String SECOND_SID = "SM2";
    public static String THIRD_SID = "SM3";

    public static String ACCOUNT_SID = "AC1234567";

    @ClassRule
    public static KuduTestHarness testHarness = new KuduTestHarness();
    public static final String BASE_TABLE_NAME = "ReportCenter.DeliveredMessages";

    public static KuduTable TABLE;

  @BeforeClass
  public static void setup() throws Exception {
    final List<ColumnSchema> columns = Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("account_sid", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("date_created", Type.UNIXTIME_MICROS).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("sid", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("mcc", Type.STRING).key(false).build(),
        new ColumnSchema.ColumnSchemaBuilder("mnc", Type.STRING).key(false).build(),
        new ColumnSchema.ColumnSchemaBuilder("error_code", Type.INT32).key(false).build());

    testHarness.getClient().createTable(BASE_TABLE_NAME, new Schema(columns),
        new org.apache.kudu.client.CreateTableOptions()
        .addHashPartitions(Arrays.asList("account_sid"), 5)
        .setNumReplicas(1));

    final AsyncKuduSession insertSession = testHarness.getAsyncClient().newSession();
    TABLE = testHarness.getClient().openTable(BASE_TABLE_NAME);

    final Upsert firstRowOp = TABLE.newUpsert();
    final PartialRow firstRowWrite = firstRowOp.getRow();
    firstRowWrite.addString("account_sid", JDBCQueryIT.ACCOUNT_SID);
    firstRowWrite.addString("sid", JDBCQueryIT.FIRST_SID);
    firstRowWrite.addTimestamp("date_created",
        new Timestamp(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1)));
    firstRowWrite.addString("mcc", "mcc1");
    firstRowWrite.addString("mnc", "mnc1");
    firstRowWrite.addInt("error_code", 1);
    insertSession.apply(firstRowOp).join();

    final Upsert secondRowOp = TABLE.newUpsert();
    final PartialRow secondRowWrite = secondRowOp.getRow();
    secondRowWrite.addString("account_sid", JDBCQueryIT.ACCOUNT_SID);
    secondRowWrite.addString("sid", JDBCQueryIT.SECOND_SID);
    Timestamp ts = new Timestamp(System.currentTimeMillis());
    secondRowWrite.addTimestamp("date_created", ts);
    secondRowWrite.addString("mcc", "mcc2");
    secondRowWrite.addString("mnc", "mnc2");
    secondRowWrite.addInt("error_code", 2);
    insertSession.apply(secondRowOp).join();

    final Upsert thirdRowOp = TABLE.newUpsert();
    final PartialRow thirdRowWrite = thirdRowOp.getRow();
    thirdRowWrite.addString("account_sid", JDBCQueryIT.ACCOUNT_SID);
    thirdRowWrite.addString("sid", JDBCQueryIT.THIRD_SID);
    thirdRowWrite.addTimestamp("date_created", ts);
    thirdRowWrite.addString("mcc", "mcc3");
    thirdRowWrite.addString("mnc", "mnc3");
    thirdRowWrite.addInt("error_code", 1);
    insertSession.apply(thirdRowOp).join();
  }

    @AfterClass
    public static void tearDown() throws Exception {
        testHarness.getClient().deleteTable(BASE_TABLE_NAME);
    }

    @Test
    public void testQuery() throws Exception {
      String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_INSERT_ENABLED,
        testHarness.getMasterAddressesAsString());
      try (Connection conn = DriverManager.getConnection(url)) {
        ResultSet rs = conn.createStatement().executeQuery("SELECT account_sid, sid FROM kudu" +
          ".\"ReportCenter.DeliveredMessages\"");
        assertTrue(rs.next());
        assertEquals("First record's account sid should match",
            ACCOUNT_SID, rs.getString("account_sid"));
        assertTrue(rs.next());
        assertEquals("Second record's account sid should match",
            ACCOUNT_SID, rs.getString("account_sid"));
        assertTrue(rs.next());
        Assert.assertEquals("Third record's account sid should match",
            ACCOUNT_SID, rs.getString("account_sid"));
        assertFalse(rs.next());
      }
    }

    @Test
    public void testProjectionWithFilter() throws Exception {
        String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_INSERT_ENABLED, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {
            String sqlFormat = "SELECT sid FROM kudu.\"ReportCenter.DeliveredMessages\" WHERE " +
                    "account_sid = '%s'";
            String sql = String.format(sqlFormat, JDBCQueryIT.ACCOUNT_SID);
            String expectedPlan = "KuduToEnumerableRel\n" +
              "  KuduProjectRel(SID=[$2])\n" +
              "    KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567])\n" +
              "      KuduQuery(table=[[kudu, ReportCenter.DeliveredMessages]])\n";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
            String plan = SqlUtil.getExplainPlan(rs);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            // since the rows are not ordered just assert that we get the expected number of rows
            rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertEquals(rs.getString("sid"), JDBCQueryIT.FIRST_SID);
            assertTrue(rs.next());
            assertEquals(rs.getString("sid"), JDBCQueryIT.SECOND_SID);
            assertTrue(rs.next());
            assertEquals(rs.getString("sid"), JDBCQueryIT.THIRD_SID);
            assertFalse(rs.next());
        }
    }

    @Test
    public void testProjectionWithFunctions() throws Exception {
        String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_INSERT_ENABLED, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {
            String sqlFormat = "SELECT mcc||mnc, mcc, mnc, mnc||mcc FROM kudu.\"ReportCenter" +
                    ".DeliveredMessages\" WHERE account_sid = '%s'";
            String sql = String.format(sqlFormat, JDBCQueryIT.ACCOUNT_SID);
            // TODO figure out if costs can be tweaked so that the two KuduProjectRel are merged
            final String expectedPlan = "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[||($t0, $t1)], expr#3=[||($t1, $t0)], EXPR$0=[$t2], MCC=[$t0], MNC=[$t1], EXPR$3=[$t3])\n" +
              "  KuduToEnumerableRel\n" +
              "    KuduProjectRel($1=[$1], $2=[$2])\n" +
              "      KuduProjectRel(ACCOUNT_SID=[$0], MCC=[$3], MNC=[$4])\n" +
              "        KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567])\n" +
              "          KuduQuery(table=[[kudu, ReportCenter.DeliveredMessages]])\n";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
            String plan = SqlUtil.getExplainPlan(rs);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            // since the rows are not ordered just assert that we get the expected number of rows
            rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertEquals("mcc1mnc1", rs.getString(1));
            assertEquals("mcc1", rs.getString(2));
            assertEquals("mnc1", rs.getString(3));
            assertEquals("mnc1mcc1", rs.getString(4));
            assertTrue(rs.next());
            assertEquals("mcc2mnc2", rs.getString(1));
            assertEquals("mcc2", rs.getString(2));
            assertEquals("mnc2", rs.getString(3));
            assertEquals("mnc2mcc2", rs.getString(4));
            assertTrue(rs.next());
            assertEquals("mcc3mnc3", rs.getString(1));
            assertEquals("mcc3", rs.getString(2));
            assertEquals("mnc3", rs.getString(3));
            assertEquals("mnc3mcc3", rs.getString(4));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testProjectionWithCountStar() throws Exception {
        String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_INSERT_ENABLED, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {
            String sqlFormat = "SELECT count(*) FROM kudu.\"ReportCenter.DeliveredMessages\" WHERE account_sid = '%s'";
            String sql = String.format(sqlFormat, JDBCQueryIT.ACCOUNT_SID);
            String expectedPlan = "EnumerableAggregate(group=[{}], EXPR$0=[COUNT()])\n" +
              "  KuduToEnumerableRel\n" +
              "    KuduProjectRel(ACCOUNT_SID=[$0])\n" +
              "      KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567])\n" +
              "        KuduQuery(table=[[kudu, ReportCenter.DeliveredMessages]])\n";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
            String plan = SqlUtil.getExplainPlan(rs);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            // since the rows are not ordered just assert that we get the expected number of rows
            rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    @Test
    public void testSortByPrimaryKey() throws Exception {
        String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_INSERT_ENABLED, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {

            String sqlFormat = "SELECT sid FROM kudu.\"ReportCenter.DeliveredMessages\" " +
                    "WHERE account_sid = '%s' ORDER BY account_sid, date_created, sid";
            String sql = String.format(sqlFormat, JDBCQueryIT.ACCOUNT_SID);
            String expectedPlan = "KuduToEnumerableRel\n" +
              "  KuduProjectRel(SID=[$2], ACCOUNT_SID=[$0], DATE_CREATED=[$1])\n" +
              "    KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], dir0=[ASC], dir1=[ASC], dir2=[ASC], groupBySorted=[false])\n" +
              "      KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567])\n" +
              "        KuduQuery(table=[[kudu, ReportCenter.DeliveredMessages]])\n";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
            String plan = SqlUtil.getExplainPlan(rs);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            // since the table has two rows each with a unique date, we expect two rows sorted by
            // date
            rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertEquals(FIRST_SID, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(SECOND_SID, rs.getString(1));
            assertTrue(rs.next());
            assertEquals(THIRD_SID, rs.getString(1));
            assertFalse(rs.next());
        }
    }

  @Test
  public void testSortOnAllGroupByColumnsWithLimit() throws Exception {
    helpTestSortedAggregation(true);
  }

  @Test
  public void testSortOnAllGroupByColumnsWithoutLimit() throws Exception {
    helpTestSortedAggregation(false);
  }

  private void helpTestSortedAggregation(boolean limit) throws SQLException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_INSERT_ENABLED, testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
        String sqlFormat = "SELECT account_sid, date_created , count(*) FROM kudu" +
                ".\"ReportCenter.DeliveredMessages\" " +
                "WHERE account_sid = '%s' GROUP BY account_sid, date_created ORDER BY " +
                "account_sid, date_created %s";
        String sql = String.format(sqlFormat, JDBCQueryIT.ACCOUNT_SID, limit ? "LIMIT 51" :
            "");
        String expectedPlanFormat = "EnumerableAggregate(group=[{0, 1}], EXPR$2=[COUNT()])\n" +
            "  KuduToEnumerableRel\n" +
            "    KuduSortRel(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC],%s " +
            "groupBySorted=[true])\n" +
            "      KuduProjectRel(ACCOUNT_SID=[$0], DATE_CREATED=[$1])\n" +
            "        KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567])\n" +
            "          KuduQuery(table=[[kudu, ReportCenter.DeliveredMessages]])\n";
        String expectedPlan = String.format(expectedPlanFormat, limit ? " fetch=[51]," : "",
            Boolean.toString(limit));
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
        String plan = SqlUtil.getExplainPlan(rs);
        assertEquals(String.format("Unexpected plan\n%s", plan),
            expectedPlan, plan);

        // since the table has three rows each with a unique date, we expect three rows
        // sorted by date
        rs = conn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(ACCOUNT_SID, rs.getString(1));
        Date d1 = rs.getDate(2);
        assertEquals(1, rs.getInt(3));
        assertTrue(rs.next());
        assertEquals(ACCOUNT_SID, rs.getString(1));
        Date d2 = rs.getDate(2);
        assertEquals(2, rs.getInt(3));
        assertFalse(rs.next());
        assertTrue(d1.before(d2));
    }
  }

  @Test
    public void testSortOnSubsetOfGroupByColumns() throws Exception {
        String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_INSERT_ENABLED, testHarness.getMasterAddressesAsString());
        try (Connection conn = DriverManager.getConnection(url)) {

            String sqlFormat = "SELECT account_sid, error_code , count(*) FROM kudu" +
                    ".\"ReportCenter.DeliveredMessages\" " +
                    "WHERE account_sid = '%s' GROUP BY account_sid, error_code ORDER BY " +
                    "account_sid  LIMIT 51";
            String sql = String.format(sqlFormat, JDBCQueryIT.ACCOUNT_SID);
            String expectedPlan = "EnumerableLimit(fetch=[51])\n" +
                    "  EnumerableSort(sort0=[$0], dir0=[ASC])\n" +
                    "    EnumerableAggregate(group=[{0, 1}], EXPR$2=[COUNT()])\n" +
                    "      KuduToEnumerableRel\n" +
                    "        KuduProjectRel(ACCOUNT_SID=[$0], ERROR_CODE=[$5])\n" +
                    "          KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567])\n" +
                    "            KuduQuery(table=[[kudu, ReportCenter.DeliveredMessages]])\n";
            ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
            String plan = SqlUtil.getExplainPlan(rs);
            assertEquals("Unexpected plan ", expectedPlan, plan);

            Map<Integer, Integer> expectedCounts =
                ImmutableMap.<Integer, Integer>builder()
                    .put(1,2)
                    .put(2,1)
                    .build();
            Map<Integer, Integer> actualCounts = new HashMap<>(2);
            rs = conn.createStatement().executeQuery(sql);
            assertTrue(rs.next());
            assertEquals(ACCOUNT_SID, rs.getString(1));
            actualCounts.put(rs.getInt(2), rs.getInt(3));
            assertTrue(rs.next());
            assertEquals(ACCOUNT_SID, rs.getString(1));
            actualCounts.put(rs.getInt(2), rs.getInt(3));
            assertFalse(rs.next());
            // the rows will not be returned in any order so just assert that we see all the
            // expected rows
            assertEquals(expectedCounts, actualCounts);
        }
    }

}
