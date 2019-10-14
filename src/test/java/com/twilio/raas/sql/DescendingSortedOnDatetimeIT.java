package com.twilio.raas.sql;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(JUnit4.class)
public final class DescendingSortedOnDatetimeIT {
  private static final Logger logger = LoggerFactory.getLogger(JDBCQueryRunnerIT.class);

  private static String FIRST_SID = "SM1234857";
  private static String SECOND_SID = "SM123485789";
  private static String THIRD_SID = "SM485789123";

  public static String ACCOUNT_SID = "AC1234567";

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();
  private static final String BASE_TABLE_NAME = "ReportCenter.AuditEvents";

  private static KuduTable TABLE;

  @BeforeClass
  public static void setup() throws Exception {
    final List<ColumnSchema> columns = Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("account_sid", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("event_date", Type.INT64).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("sid", Type.STRING).key(true).build());

    testHarness.getClient().createTable(BASE_TABLE_NAME, new Schema(columns),
        new org.apache.kudu.client.CreateTableOptions()
            .addHashPartitions(Arrays.asList("account_sid"), 5)

            .setNumReplicas(1));
    final AsyncKuduSession insertSession = testHarness.getAsyncClient().newSession();
    TABLE = testHarness.getClient().openTable(BASE_TABLE_NAME);

    final Upsert firstRowOp = TABLE.newUpsert();
    final PartialRow firstRowWrite = firstRowOp.getRow();
    firstRowWrite.addString("account_sid", JDBCQueryRunnerIT.ACCOUNT_SID);
    firstRowWrite.addLong("event_date", Long.MAX_VALUE - (Instant.parse("2019-01-02T01:00:00.000Z").toEpochMilli()*1000L));
    firstRowWrite.addString("sid", DescendingSortedOnDatetimeIT.FIRST_SID);
    insertSession.apply(firstRowOp).join();

    final Upsert secondRowOp = TABLE.newUpsert();
    final PartialRow secondRowWrite = secondRowOp.getRow();
    secondRowWrite.addString("account_sid", JDBCQueryRunnerIT.ACCOUNT_SID);
    secondRowWrite.addLong("event_date", Long.MAX_VALUE - (Instant.parse("2019-01-02T02:25:00.000Z").toEpochMilli()*1000L));
    secondRowWrite.addString("sid", "SM123485789");
    insertSession.apply(secondRowOp).join();

    final Upsert thirdRowOp = TABLE.newUpsert();
    final PartialRow thirdRowWrite = thirdRowOp.getRow();
    thirdRowWrite.addString("account_sid", JDBCQueryRunnerIT.ACCOUNT_SID);
    thirdRowWrite.addLong("event_date", Long.MAX_VALUE - (Instant.parse("2019-01-01T01:00:00.000Z").toEpochMilli()*1000L));
    thirdRowWrite.addString("sid", DescendingSortedOnDatetimeIT.THIRD_SID);
    insertSession.apply(thirdRowOp).join();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testHarness.getClient().deleteTable(BASE_TABLE_NAME);
  }

  @Test
  public void testQueryWithSortDesc() throws Exception {
    try (final JDBCQueryRunner runner = new JDBCQueryRunner(testHarness.getMasterAddressesAsString(), 1)) {
      List<Map<String, Object>> records = runner
          .executeSql("SELECT account_sid, event_date, sid FROM kudu.\"ReportCenter.AuditEvents\" order by event_date desc", (rs -> {
            final Map<String, Object> record = new HashMap<>();
            try {
              record.put("account_sid", rs.getString(1));
              record.put("event_date", rs.getLong(2));
              record.put("sid", rs.getString(3));
            }
            catch (SQLException failed) {
              //swallow it
            }
            return record;
          }))
          .toCompletableFuture().get();
      Assert.assertEquals("should get the rows back",
          3, records.size());
      Assert.assertEquals("First record's account sid should match",
          ACCOUNT_SID, records.get(0).get("account_sid"));
      Assert.assertTrue("First record's datetime is later than second's",
          Instant.ofEpochMilli((Long)records.get(0).get("event_date")).isAfter(Instant.ofEpochMilli((Long)records.get(1).get("event_date"))));
      Assert.assertTrue("Second record's datetime is later than first's",
          Instant.ofEpochMilli((Long)records.get(0).get("event_date")).isAfter(Instant.ofEpochMilli((Long)records.get(1).get("event_date"))));
    }
  }

  @Test
  public void testQueryWithSortAsc() throws Exception {
    try (final JDBCQueryRunner runner = new JDBCQueryRunner(testHarness.getMasterAddressesAsString(), 1)) {
      List<Map<String, Object>> records = runner
          .executeSql("SELECT account_sid, event_date, sid FROM kudu.\"ReportCenter.AuditEvents\" order by event_date asc", (rs -> {
            final Map<String, Object> record = new HashMap<>();
            try {
              record.put("account_sid", rs.getString(1));
              record.put("event_date", rs.getLong(2));
              record.put("sid", rs.getString(3));
            }
            catch (SQLException failed) {
              //swallow it
            }
            return record;
          }))
          .toCompletableFuture().get();
      Assert.assertEquals("should get the rows back",
          3, records.size());
      Assert.assertEquals("First record's account sid should match",
          ACCOUNT_SID, records.get(0).get("account_sid"));
      Assert.assertTrue("First record's datetime is earlier than second's",
          Instant.ofEpochMilli((Long)records.get(0).get("event_date")).isBefore(Instant.ofEpochMilli((Long)records.get(1).get("event_date"))));
      Assert.assertTrue("Second record's datetime is earlier than first's",
          Instant.ofEpochMilli((Long)records.get(0).get("event_date")).isBefore(Instant.ofEpochMilli((Long)records.get(1).get("event_date"))));
    }
  }

  @Test
  public void testQueryWithPredicates() throws Exception {
    try (final JDBCQueryRunner runner = new JDBCQueryRunner(testHarness.getMasterAddressesAsString(), 1)) {
      List<Map<String, Object>> records = runner
          .executeSql("SELECT account_sid, event_date, sid FROM kudu.\"ReportCenter.AuditEvents\" where event_date >= TIMESTAMP'2019-01-01 00:00:00' and event_date < TIMESTAMP'2019-01-02 00:00:00'", (rs -> {
            final Map<String, Object> record = new HashMap<>();
            try {
              record.put("account_sid", rs.getString(1));
              record.put("event_date", rs.getLong(2));
              record.put("sid", rs.getString(3));
            }
            catch (SQLException failed) {
              //swallow it
            }
            return record;
          }))
          .toCompletableFuture().get();
      Assert.assertEquals("should get the rows back",
          1, records.size());
      Assert.assertEquals("First record's account sid should match",
          ACCOUNT_SID, records.get(0).get("account_sid"));
      Assert.assertTrue("Record's datetime is of third upserted record",
          Instant.ofEpochMilli((Long)records.get(0).get("event_date")).equals(Instant.parse("2019-01-01T01:00:00.000Z")));
    }
  }

  @Test
  public void testQueryWithPredicatesAndSortAsc() throws Exception {
    try (final JDBCQueryRunner runner = new JDBCQueryRunner(testHarness.getMasterAddressesAsString(), 1)) {
      List<Map<String, Object>> records = runner
          .executeSql("SELECT account_sid, event_date, sid FROM kudu.\"ReportCenter.AuditEvents\" where event_date >= TIMESTAMP'2019-01-02 00:00:00' and event_date < TIMESTAMP'2019-01-03 00:00:00' order by event_date asc", (rs -> {
            final Map<String, Object> record = new HashMap<>();
            try {
              record.put("account_sid", rs.getString(1));
              record.put("event_date", rs.getLong(2));
              record.put("sid", rs.getString(3));
            }
            catch (SQLException failed) {
              //swallow it
            }
            return record;
          }))
          .toCompletableFuture().get();
      Assert.assertEquals("should get the rows back",
          2, records.size());
      Assert.assertEquals("First record's account sid should match",
          ACCOUNT_SID, records.get(0).get("account_sid"));
      Assert.assertTrue("First record's datetime is earlier than second's",
          Instant.ofEpochMilli((Long)records.get(0).get("event_date")).isBefore(Instant.ofEpochMilli((Long)records.get(1).get("event_date"))));
    }
  }

  @Test
  public void testQueryWithPredicatesAndSortDesc() throws Exception {
    try (final JDBCQueryRunner runner = new JDBCQueryRunner(testHarness.getMasterAddressesAsString(), 1)) {
      List<Map<String, Object>> records = runner
          .executeSql("SELECT account_sid, event_date, sid FROM kudu.\"ReportCenter.AuditEvents\" where event_date >= TIMESTAMP'2019-01-02 00:00:00' and event_date < TIMESTAMP'2019-01-03 00:00:00' order by event_date desc", (rs -> {
            final Map<String, Object> record = new HashMap<>();
            try {
              record.put("account_sid", rs.getString(1));
              record.put("event_date", rs.getLong(2));
              record.put("sid", rs.getString(3));
            }
            catch (SQLException failed) {
              //swallow it
            }
            return record;
          }))
          .toCompletableFuture().get();
      Assert.assertEquals("should get the rows back",
          2, records.size());
      Assert.assertEquals("First record's account sid should match",
          ACCOUNT_SID, records.get(0).get("account_sid"));
      Assert.assertTrue("First record's datetime is later than second's",
          Instant.ofEpochMilli((Long)records.get(0).get("event_date")).isAfter(Instant.ofEpochMilli((Long)records.get(1).get("event_date"))));
    }
  }
}
