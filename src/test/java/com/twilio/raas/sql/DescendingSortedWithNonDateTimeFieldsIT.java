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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class DescendingSortedWithNonDateTimeFieldsIT {
  private static final Logger logger = LoggerFactory.getLogger(JDBCQueryRunnerIT.class);

  private static String ACCOUNT_SID = "AC1234567";

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();
  private static final String descendingSortTableName = "DescendingSortTestTable";
  private static final String customTemplate = "jdbc:calcite:model=inline:{version: '1.0',defaultSchema:'kudu',schemas:[{name: 'kudu',type:'custom',factory:'com.twilio.raas.sql.KuduSchemaFactory',operand:{connect:'%s',kuduTableConfigs:[{tableName: 'DescendingSortTestTable', descendingSortedFields:['reverse_byte_field', 'reverse_short_field', 'reverse_int_field', 'reverse_long_field']}]}}]};caseSensitive=false;timeZone=UTC";

  private static KuduTable descendingSortTestTable;

  @BeforeClass
  public static void setup() throws Exception {
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
    firstRowWrite.addByte("reverse_byte_field", (byte)(Byte.MAX_VALUE - new Byte("3")));
    firstRowWrite.addShort("reverse_short_field", (short)(Short.MAX_VALUE - new Short("32")));
    firstRowWrite.addInt("reverse_int_field", Integer.MAX_VALUE - 100);
    firstRowWrite.addLong("reverse_long_field", Long.MAX_VALUE - 1000L);
    firstRowWrite.addString("resource_type", "message-body");
    insertSession.apply(firstRowOp).join();

    final Upsert secondRowOp = descendingSortTestTable.newUpsert();
    final PartialRow secondRowWrite = secondRowOp.getRow();
    secondRowWrite.addString("account_sid", JDBCQueryRunnerIT.ACCOUNT_SID);
    secondRowWrite.addByte("reverse_byte_field", (byte)(Byte.MAX_VALUE - new Byte("4")));
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
  public void testDescendingSortWithReverseSortedFields() throws Exception {
    try (final JDBCQueryRunner runner = new JDBCQueryRunner(customTemplate, testHarness.getMasterAddressesAsString(), 1)) {
      String url = String.format(customTemplate, testHarness.getMasterAddressesAsString());
      try (Connection conn = DriverManager.getConnection(url)) {
        String firstBatchSqlFormat = "SELECT * FROM kudu.\"DescendingSortTestTable\""
            + "WHERE account_sid = '%s' "
            + "ORDER BY account_sid asc, reverse_byte_field desc, reverse_short_field desc, reverse_int_field desc, reverse_long_field desc";
        String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

        // verify plan
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
        String plan = SqlUtil.getExplainPlan(rs);
        String expectedPlanFormat = "KuduToEnumerableRel\n" +
            "  KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], sort3=[$3], sort4=[$4], dir0=[ASC], dir1=[DESC], dir2=[DESC], dir3=[DESC], dir4=[DESC])\n" +
            "    KuduFilterRel(Scan 1=[account_sid EQUAL AC1234567])\n" +
            "      KuduQuery(table=[[kudu, DescendingSortTestTable]])\n";
        String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
        assertEquals("Unexpected plan ", expectedPlan, plan);
        rs = conn.createStatement().executeQuery(firstBatchSql);

        assertTrue(rs.next());
        assertEquals("Mismatched byte", new Byte("4"), new Byte(rs.getByte("reverse_byte_field")));
        assertEquals("Mismatched short", new Short("33"), new Short(rs.getShort("reverse_short_field")));
        assertEquals("Mismatched int", 101, rs.getInt("reverse_int_field"));
        assertEquals("Mismatched long", 1001L, rs.getLong("reverse_long_field"));

        assertTrue(rs.next());
        assertEquals("Mismatched byte", new Byte("3"), new Byte(rs.getByte("reverse_byte_field")));
        assertEquals("Mismatched short", new Short("32"), new Short(rs.getShort("reverse_short_field")));
        assertEquals("Mismatched int", 100, rs.getInt("reverse_int_field"));
        assertEquals("Mismatched long", 1000L, rs.getLong("reverse_long_field"));
      }
    }
  }

  @Test
  public void testAscendingSortWithReverseSortedFields() throws Exception {
    try (final JDBCQueryRunner runner = new JDBCQueryRunner(customTemplate, testHarness.getMasterAddressesAsString(), 1)) {
      String url = String.format(customTemplate, testHarness.getMasterAddressesAsString());
      try (Connection conn = DriverManager.getConnection(url)) {
        String firstBatchSqlFormat = "SELECT * FROM kudu.\"DescendingSortTestTable\""
            + "ORDER BY account_sid, reverse_byte_field, reverse_short_field, reverse_int_field, reverse_long_field asc";
        String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

        // verify plan
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
        String plan = SqlUtil.getExplainPlan(rs);
        String expectedPlanFormat = "EnumerableSort(sort0=[$0], sort1=[$1], sort2=[$2], sort3=[$3], sort4=[$4], dir0=[ASC], dir1=[ASC], dir2=[ASC], dir3=[ASC], dir4=[ASC])\n" +
            "  KuduToEnumerableRel\n" +
            "    KuduQuery(table=[[kudu, DescendingSortTestTable]])\n";
        String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
        assertEquals("Unexpected plan ", expectedPlan, plan);
        rs = conn.createStatement().executeQuery(firstBatchSql);

        assertTrue(rs.next());
        assertEquals("Mismatched byte", new Byte("3"), new Byte(rs.getByte("reverse_byte_field")));
        assertEquals("Mismatched short", new Short("32"), new Short(rs.getShort("reverse_short_field")));
        assertEquals("Mismatched int", 100, rs.getInt("reverse_int_field"));
        assertEquals("Mismatched long", 1000L, rs.getLong("reverse_long_field"));

        assertTrue(rs.next());
        assertEquals("Mismatched byte", new Byte("4"), new Byte(rs.getByte("reverse_byte_field")));
        assertEquals("Mismatched short", new Short("33"), new Short(rs.getShort("reverse_short_field")));
        assertEquals("Mismatched int", 101, rs.getInt("reverse_int_field"));
        assertEquals("Mismatched long", 1001L, rs.getLong("reverse_long_field"));
      }
    }
  }

  @Test
  public void testDescendingSortWithFilter() throws Exception {
    try (final JDBCQueryRunner runner = new JDBCQueryRunner(customTemplate, testHarness.getMasterAddressesAsString(), 1)) {
      String url = String.format(customTemplate, testHarness.getMasterAddressesAsString());
      try (Connection conn = DriverManager.getConnection(url)) {
        String firstBatchSqlFormat = "SELECT * FROM kudu.\"DescendingSortTestTable\""
            + "WHERE account_sid = '%s' and reverse_byte_field > CAST(3 AS TINYINT) and reverse_short_field > CAST(32 AS SMALLINT) and reverse_int_field > 100 and reverse_long_field > CAST(1000 AS BIGINT) "
            + "ORDER BY account_sid asc, reverse_byte_field desc, reverse_short_field desc, reverse_int_field desc, reverse_long_field desc";
        String firstBatchSql = String.format(firstBatchSqlFormat, ACCOUNT_SID);

        // verify plan
        ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + firstBatchSql);
        String plan = SqlUtil.getExplainPlan(rs);
        String expectedPlanFormat = "KuduToEnumerableRel\n" +
            "  KuduSortRel(sort0=[$0], sort1=[$1], sort2=[$2], sort3=[$3], sort4=[$4], dir0=[ASC], dir1=[DESC], dir2=[DESC], dir3=[DESC], dir4=[DESC])\n" +
            "    KuduFilterRel(Scan 1=[account_sid EQUAL AC1234567 , reverse_byte_field GREATER 3 , reverse_short_field GREATER 32 , reverse_int_field GREATER 100 , reverse_long_field GREATER 1000])\n" +
            "      KuduQuery(table=[[kudu, DescendingSortTestTable]])\n";
        String expectedPlan = String.format(expectedPlanFormat, ACCOUNT_SID);
        assertEquals("Unexpected plan ", expectedPlan, plan);
        rs = conn.createStatement().executeQuery(firstBatchSql);

        assertTrue(rs.next());
        assertEquals("Mismatched byte", new Byte("4"), new Byte(rs.getByte("reverse_byte_field")));
        assertEquals("Mismatched short", new Short("33"), new Short(rs.getShort("reverse_short_field")));
        assertEquals("Mismatched int", 101, rs.getInt("reverse_int_field"));
        assertEquals("Mismatched long", 1001L, rs.getLong("reverse_long_field"));
      }
    }
  }
}
