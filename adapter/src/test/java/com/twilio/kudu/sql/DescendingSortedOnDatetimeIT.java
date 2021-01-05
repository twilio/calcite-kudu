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

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public final class DescendingSortedOnDatetimeIT {

  private static String FIRST_SID = "SM1234857";
  private static String SECOND_SID = "SM123485789";
  private static String THIRD_SID = "SM485789123";

  private static String ACCOUNT_SID = "AC1234567";

  private static final String EVENT_DATE_FIELD = "event_date";

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();
  private static final String BASE_TABLE_NAME = "Test.Events";

  private static KuduTable TABLE;
  private static String JDBC_URL;

  private static void validateRow(ResultSet rs, long expectedTimestamp, String expectedSid) throws SQLException {
    assertEquals("Mismatched account sid", ACCOUNT_SID, rs.getString("account_sid"));
    assertEquals("Mismatched event_date", expectedTimestamp,
        rs.getTimestamp(EVENT_DATE_FIELD).toInstant().toEpochMilli());
    assertEquals("Mismatched sid", expectedSid, rs.getString("sid"));
  }

  @BeforeClass
  public static void setup() throws Exception {
    final List<ColumnSchema> columns = Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("account_sid", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("event_date", Type.UNIXTIME_MICROS).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("sid", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("resource_type", Type.STRING).build());

    Schema schema = new Schema(columns);
    PartialRow row1 = schema.newPartialRow();
    row1.addTimestamp(EVENT_DATE_FIELD, new Timestamp(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS
        - (Instant.parse("2018-12-31T00:00:00.000Z").toEpochMilli())));
    PartialRow row2 = schema.newPartialRow();
    row2.addTimestamp(EVENT_DATE_FIELD, new Timestamp(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS
        - (Instant.parse("2019-01-01T00:00:00.000Z").toEpochMilli())));
    PartialRow row3 = schema.newPartialRow();
    row3.addTimestamp(EVENT_DATE_FIELD, new Timestamp(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS
        - (Instant.parse("2019-01-02T00:00:00.000Z").toEpochMilli())));
    PartialRow row4 = schema.newPartialRow();
    row4.addTimestamp(EVENT_DATE_FIELD, new Timestamp(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS
        - (Instant.parse("2019-01-03T00:00:00.000Z").toEpochMilli())));

    testHarness.getClient().createTable(BASE_TABLE_NAME, schema,
        new org.apache.kudu.client.CreateTableOptions().addHashPartitions(Arrays.asList("account_sid"), 5)
            .setRangePartitionColumns(Arrays.asList("event_date")).addRangePartition(row4, row3)
            .addRangePartition(row3, row2).addRangePartition(row2, row1).setNumReplicas(1));
    final AsyncKuduSession insertSession = testHarness.getAsyncClient().newSession();
    TABLE = testHarness.getClient().openTable(BASE_TABLE_NAME);

    JDBC_URL = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED, KuduSchemaFactory.class.getName(),
        testHarness.getMasterAddressesAsString());

    final Upsert firstRowOp = TABLE.newUpsert();
    final PartialRow firstRowWrite = firstRowOp.getRow();
    firstRowWrite.addString("account_sid", JDBCQueryIT.ACCOUNT_SID);
    firstRowWrite.addTimestamp("event_date", new Timestamp(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS
        - (Instant.parse("2019-01-02T01:00:00.000Z").toEpochMilli())));
    firstRowWrite.addString("sid", DescendingSortedOnDatetimeIT.FIRST_SID);
    firstRowWrite.addString("resource_type", "message-body");
    insertSession.apply(firstRowOp).join();

    final Upsert secondRowOp = TABLE.newUpsert();
    final PartialRow secondRowWrite = secondRowOp.getRow();
    secondRowWrite.addString("account_sid", JDBCQueryIT.ACCOUNT_SID);
    secondRowWrite.addTimestamp("event_date", new Timestamp(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS
        - (Instant.parse("2019-01-02T02:25:00.000Z").toEpochMilli())));
    secondRowWrite.addString("sid", DescendingSortedOnDatetimeIT.SECOND_SID);
    secondRowWrite.addString("resource_type", "recording");
    insertSession.apply(secondRowOp).join();

    final Upsert thirdRowOp = TABLE.newUpsert();
    final PartialRow thirdRowWrite = thirdRowOp.getRow();
    thirdRowWrite.addString("account_sid", JDBCQueryIT.ACCOUNT_SID);
    thirdRowWrite.addTimestamp("event_date", new Timestamp(CalciteKuduTable.EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS
        - (Instant.parse("2019-01-01T01:00:00.000Z").toEpochMilli())));
    thirdRowWrite.addString("sid", DescendingSortedOnDatetimeIT.THIRD_SID);
    thirdRowWrite.addString("resource_type", "sms-geographic-permission");
    insertSession.apply(thirdRowOp).join();
  }

  public static class KuduSchemaFactory extends BaseKuduSchemaFactory {
    private static final Map<String, KuduTableMetadata> kuduTableConfigMap = new ImmutableMap.Builder<String, KuduTableMetadata>()
        .put("Test.Events", new KuduTableMetadata.KuduTableMetadataBuilder()
            .setDescendingOrderedColumnNames(Lists.newArrayList("event_date")).build())
        .build();

    // Public singleton, per factory contract.
    public static final KuduSchemaFactory INSTANCE = new KuduSchemaFactory(kuduTableConfigMap);

    public KuduSchemaFactory(Map<String, KuduTableMetadata> kuduTableConfigMap) {
      super(kuduTableConfigMap);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testHarness.getClient().deleteTable(BASE_TABLE_NAME);
  }

  @Test
  public void testQueryWithSortDesc() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement()
          .executeQuery("SELECT account_sid, event_date, sid " + "FROM kudu.\"Test.Events\" order by event_date desc");
      assertTrue(rs.next());
      assertEquals("Record's account sid should match", ACCOUNT_SID, rs.getString("account_sid"));
      Date d1 = rs.getDate("event_date");
      assertTrue(rs.next());
      assertEquals("Record's account sid should match", ACCOUNT_SID, rs.getString("account_sid"));
      Date d2 = rs.getDate("event_date");
      assertTrue(rs.next());
      assertEquals("Record's account sid should match", ACCOUNT_SID, rs.getString("account_sid"));
      Date d3 = rs.getDate("event_date");
      assertFalse(rs.next());
      assertTrue("First record's datetime should be later than second's", d1.after(d2));
      assertTrue("Second record's datetime should be later than first's", d2.after(d3));
    }
  }

  @Test
  public void testQueryWithSortAsc() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement()
          .executeQuery("SELECT account_sid, event_date, sid " + "FROM kudu.\"Test.Events\" order by event_date asc");
      assertTrue(rs.next());
      assertEquals("Record's account sid should match", ACCOUNT_SID, rs.getString("account_sid"));
      Date d1 = rs.getDate("event_date");
      assertTrue(rs.next());
      assertEquals("Record's account sid should match", ACCOUNT_SID, rs.getString("account_sid"));
      Date d2 = rs.getDate("event_date");
      assertTrue(rs.next());
      assertEquals("Record's account sid should match", ACCOUNT_SID, rs.getString("account_sid"));
      Date d3 = rs.getDate("event_date");
      assertFalse(rs.next());
      assertTrue("First record's datetime should be earlier than second's", d1.before(d2));
      assertTrue("Second record's datetime should be earlier than first's", d2.before(d3));
    }
  }

  @Test
  public void testQueryWithPredicates() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement()
          .executeQuery("SELECT account_sid, event_date, sid "
              + "FROM kudu.\"Test.Events\" where event_date >= TIMESTAMP'2019-01-01 "
              + "00:00:00' and event_date < TIMESTAMP'2019-01-02 00:00:00'");
      assertTrue(rs.next());
      assertEquals("Record's account sid should match", ACCOUNT_SID, rs.getString("account_sid"));
      assertEquals(String.format("Record's datetime is of third upserted record: %d", rs.getLong("event_date")),
          Instant.parse("2019-01-01T01:00:00.000Z"), Instant.ofEpochMilli(rs.getLong("event_date")));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testQueryWithPredicatesAndSortAsc() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement()
          .executeQuery("SELECT account_sid, event_date, sid "
              + "FROM kudu.\"Test.Events\" where event_date >= TIMESTAMP'2019-01-02 "
              + "00:00:00' and event_date < TIMESTAMP'2019-01-03 00:00:00' order by event_date asc");
      assertTrue(rs.next());
      assertEquals("Record's account sid should match", ACCOUNT_SID, rs.getString("account_sid"));
      Date d1 = rs.getDate("event_date");
      assertTrue(rs.next());
      assertEquals("Record's account sid should match", ACCOUNT_SID, rs.getString("account_sid"));
      Date d2 = rs.getDate("event_date");
      assertFalse(rs.next());
      assertTrue("First record's datetime should be earlier than second's", d1.before(d2));
    }
  }

  @Test
  public void testQueryWithPredicatesAndSortDesc() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement()
          .executeQuery("SELECT account_sid, event_date, sid "
              + "FROM kudu.\"Test.Events\" where event_date >= TIMESTAMP'2019-01-02 "
              + "00:00:00' and event_date < TIMESTAMP'2019-01-03 00:00:00' order by event_date desc");
      assertTrue(rs.next());
      assertEquals("Record's account sid should match", ACCOUNT_SID, rs.getString("account_sid"));
      Date d1 = rs.getDate("event_date");
      assertTrue(rs.next());
      assertEquals("Record's account sid should match", ACCOUNT_SID, rs.getString("account_sid"));
      Date d2 = rs.getDate("event_date");
      assertFalse(rs.next());
      assertTrue("First record's datetime should be later than second's", d1.after(d2));
    }
  }

  @Test
  public void testSortWithFilter() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String firstBatchSqlFormat = "SELECT * FROM kudu.\"Test.Events\" " + "WHERE account_sid = '%s' "
          + "ORDER BY event_date desc, account_sid asc ";
      String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC], groupBySorted=[false])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567])\n"
          + "      KuduQuery(table=[[kudu, Test.Events]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
      assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);
      rs = conn.createStatement().executeQuery(firstBatchSql);

      assertTrue(rs.next());
      validateRow(rs, 1546395900000L, DescendingSortedOnDatetimeIT.SECOND_SID);
      assertTrue(rs.next());
      validateRow(rs, 1546390800000L, DescendingSortedOnDatetimeIT.FIRST_SID);
      assertTrue(rs.next());
      validateRow(rs, 1546304400000L, DescendingSortedOnDatetimeIT.THIRD_SID);
    }
  }

  @Test
  public void testSortWithFilterAndColumns() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String firstBatchSqlFormat = "SELECT sid, account_sid, event_date FROM kudu.\"Test.Events\" "
          + "WHERE account_sid = '%s' " + "ORDER BY event_date desc, account_sid asc ";
      String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);

      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduProjectRel(SID=[$2], ACCOUNT_SID=[$0], EVENT_DATE=[$1])\n"
          + "    KuduSortRel(sort0=[$1], sort1=[$0], dir0=[DESC], dir1=[ASC], groupBySorted=[false])\n"
          + "      KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567])\n"
          + "        KuduQuery(table=[[kudu, Test.Events]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
      assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);
      rs = conn.createStatement().executeQuery(firstBatchSql);

      assertTrue(rs.next());
      validateRow(rs, 1546395900000L, DescendingSortedOnDatetimeIT.SECOND_SID);
      assertTrue(rs.next());
      validateRow(rs, 1546390800000L, DescendingSortedOnDatetimeIT.FIRST_SID);
      assertTrue(rs.next());
      validateRow(rs, 1546304400000L, DescendingSortedOnDatetimeIT.THIRD_SID);
    }
  }

  @Test
  public void testAscendingSortOnDescendingFieldWithFilter() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String firstBatchSqlFormat = "SELECT * FROM kudu.\"Test.Events\" " + "WHERE account_sid = '%s' "
          + "ORDER BY event_date asc ";
      String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "EnumerableSort(sort0=[$1], dir0=[ASC])\n" + "  KuduToEnumerableRel\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567])\n"
          + "      KuduQuery(table=[[kudu, Test.Events]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
      assertEquals("Unexpected plan ", expectedPlan, plan);
      rs = conn.createStatement().executeQuery(firstBatchSql);

      assertTrue(rs.next());
      validateRow(rs, 1546304400000L, DescendingSortedOnDatetimeIT.THIRD_SID);
      assertTrue(rs.next());
      validateRow(rs, 1546390800000L, DescendingSortedOnDatetimeIT.FIRST_SID);
      assertTrue(rs.next());
      validateRow(rs, 1546395900000L, DescendingSortedOnDatetimeIT.SECOND_SID);
    }
  }

  @Test
  public void testQueryNoRows() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String sql = "SELECT * FROM kudu.\"Test.Events\" " + "WHERE event_date < TIMESTAMP'2018-01-01 00:00:00'";

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduFilterRel(ScanToken 1=[event_date LESS 1514764800000000])\n"
          + "    KuduQuery(table=[[kudu, Test.Events]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
      assertEquals("Unexpected plan ", expectedPlan, plan);

      rs = conn.createStatement().executeQuery(sql);
      assertFalse(rs.next());
    }
  }

  @Test
  public void testSortWithFilterAndLimit() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String firstBatchSqlFormat = "SELECT * FROM kudu.\"Test.Events\" "
          + "WHERE account_sid = '%s' and event_date <= TIMESTAMP'2019-01-02 00:00:00' " + "ORDER BY event_date desc "
          + "LIMIT 1";
      String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$1], dir0=[DESC], fetch=[1], groupBySorted=[false])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567, event_date LESS_EQUAL 1546387200000000])\n"
          + "      KuduQuery(table=[[kudu, Test.Events]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
      assertEquals("Unexpected plan ", expectedPlan, plan);
      rs = conn.createStatement().executeQuery(firstBatchSql);

      assertTrue(rs.next());
      validateRow(rs, 1546304400000L, DescendingSortedOnDatetimeIT.THIRD_SID);
    }
  }

  @Test
  public void testSortOnNonPkFieldWithFilter() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String firstBatchSqlFormat = "SELECT * FROM kudu.\"Test.Events\" " + "WHERE account_sid = '%s' "
          + "ORDER BY event_date desc, resource_type asc ";
      String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "EnumerableSort(sort0=[$1], sort1=[$3], dir0=[DESC], dir1=[ASC])\n"
          + "  KuduToEnumerableRel\n" + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567])\n"
          + "      KuduQuery(table=[[kudu, Test.Events]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
      assertEquals("Unexpected plan ", expectedPlan, plan);
      rs = conn.createStatement().executeQuery(firstBatchSql);

      assertTrue(rs.next());
      validateRow(rs, 1546395900000L, DescendingSortedOnDatetimeIT.SECOND_SID);
      assertTrue(rs.next());
      validateRow(rs, 1546390800000L, DescendingSortedOnDatetimeIT.FIRST_SID);
      assertTrue(rs.next());
      validateRow(rs, 1546304400000L, DescendingSortedOnDatetimeIT.THIRD_SID);
    }
  }

  @Test
  public void testSortOnSid() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      // Because the first two columns of the primary key are filtered to exact values
      // this should
      // pass.
      String firstBatchSqlFormat = "SELECT * FROM kudu.\"Test.Events\" "
          + "WHERE account_sid = '%s' and event_date = TIMESTAMP'2019-01-01 00:00:00' " + "ORDER BY sid asc";

      String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "KuduToEnumerableRel\n"
          + "  KuduSortRel(sort0=[$2], dir0=[ASC], groupBySorted=[false])\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567, event_date EQUAL 1546300800000000])\n"
          + "      KuduQuery(table=[[kudu, Test.Events]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
      assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);
      rs = conn.createStatement().executeQuery(firstBatchSql);
    }
  }

  @Test
  public void testSortOnSidWithDateRange() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      // Because the event_date doesn't have an exact filter, we should not apply kudu
      // sort
      // This test demostrates the need for the while loop in the Sort Rule.
      String firstBatchSqlFormat = "SELECT * FROM kudu.\"Test.Events\" "
          + "WHERE account_sid = '%s' and event_date <= TIMESTAMP'2019-01-01 00:00:00' " + "ORDER BY sid asc";

      String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

      // verify plan
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
      String plan = SqlUtil.getExplainPlan(rs);
      String expectedPlanFormat = "EnumerableSort(sort0=[$2], dir0=[ASC])\n" + "  KuduToEnumerableRel\n"
          + "    KuduFilterRel(ScanToken 1=[account_sid EQUAL AC1234567, event_date LESS_EQUAL 1546300800000000])\n"
          + "      KuduQuery(table=[[kudu, Test.Events]])\n";
      String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
      assertEquals(String.format("Unexpected plan\n%s", plan), expectedPlan, plan);
      rs = conn.createStatement().executeQuery(firstBatchSql);
    }
  }
}
