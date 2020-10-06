package com.twilio.raas.sql.parser;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.twilio.raas.sql.JDBCUtil;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.PartitionSchema;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class KuduDDLIT {

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();

  @Test
  public void testCreateMaterializedViewCountAggregate() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {

      String ddl = "CREATE TABLE \"MY_TABLE\" (" +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
              "UNIXTIME_MICROS_COL TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
              "is the timestamp', " +
              "\"int64_col\" BIGINT DEFAULT 1234567890, " +
              "INT8_COL TINYINT not null DEFAULT -128," +
              "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
              "INT32_COL INTEGER not null DEFAULT -2147483648, " +
              "BINARY_COL VARBINARY DEFAULT x'AB'," +
              "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
              "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
              "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
              "PRIMARY KEY (STRING_COL, UNIXTIME_MICROS_COL, \"int64_col\"))" +
              "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
              "NUM_REPLICAS 1 " +
              "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);

      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE\" " +
              "AS SELECT STRING_COL, UNIXTIME_MICROS_COL, COUNT(INT32_COL) " +
              "FROM \"MY_TABLE\" " +
              "GROUP BY STRING_COL, UNIXTIME_MICROS_COL";
      conn.createStatement().execute(ddl2);
      // validate the table can be queried
      ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM \"my_schema.MY_CUBE\"");
      assertFalse(rs.next());
    }

    KuduClient client = testHarness.getClient();
    KuduTable kuduTable = client.openTable("my_schema.MY_CUBE");

    // validate hash partitioning
    List<PartitionSchema.HashBucketSchema> hashBucketSchemas =
            kuduTable.getPartitionSchema().getHashBucketSchemas();
    assertEquals("Unexpected number of hash partitions", 1, hashBucketSchemas.size());
    assertEquals("Unexpected number of hash buckets", 17, hashBucketSchemas.get(0).getNumBuckets());
    assertEquals("Unexpected hash partition columns", Lists.newArrayList(0),
            hashBucketSchemas.get(0).getColumnIds());

    List<Integer> ids = kuduTable.getPartitionSchema().getRangeSchema().getColumnIds();
    assertEquals("expected number of replicas", 1, ids.stream().count());
    // validate replicas
    assertEquals("Unexpected number of replicas", 1, kuduTable.getNumReplicas());

    // validate table options
    Map<String, String> expectedConfig = ImmutableMap.<String, String>builder().
            put("kudu.table.history_max_age_sec", "7200").
            build();
    assertEquals("Unexpected extra configs", expectedConfig, kuduTable.getExtraConfig());

    // validate primary key
    assertEquals(2, kuduTable.getSchema().getPrimaryKeyColumnCount());
    List<ColumnSchema> columnSchemas = kuduTable.getSchema().getPrimaryKeyColumns();
    ColumnSchema idColSchema = columnSchemas.get(0);
    assertNotNull(idColSchema);
    assertEquals("STRING_COL", idColSchema.getName());
    assertEquals(ColumnSchema.Encoding.PREFIX_ENCODING, idColSchema.getEncoding());
    assertEquals(ColumnSchema.CompressionAlgorithm.LZ4, idColSchema.getCompressionAlgorithm());
    assertEquals(5000, idColSchema.getDesiredBlockSize());
    ColumnSchema timestampCol = columnSchemas.get(1);
    assertEquals("UNIXTIME_MICROS_COL", timestampCol.getName());
    assertEquals("this column is the timestamp", timestampCol.getComment());

    //validate column attributes
    Schema schema = kuduTable.getSchema();
    validateColumnSchema(schema, "STRING_COL",  Type.STRING, false, "abc");
    validateColumnSchema(schema, "UNIXTIME_MICROS_COL",  Type.UNIXTIME_MICROS, false, 1234567890l);
    validateColumnSchema(schema, "COUNT_INT32_COL",  Type.INT64, false, null);
  }

  @Test
  public void testCreateMaterializedViewMultipleAggregates() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {

      String ddl = "CREATE TABLE \"MY_TABLE_24\" (" +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
              "UNIXTIME_MICROS_COL TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
              "is the timestamp', " +
              "\"int64_col\" BIGINT DEFAULT 1234567890, " +
              "INT8_COL TINYINT not null DEFAULT -128," +
              "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
              "INT32_COL INTEGER not null DEFAULT -2147483648, " +
              "BINARY_COL VARBINARY DEFAULT x'AB'," +
              "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
              "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
              "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
              "PRIMARY KEY (STRING_COL, UNIXTIME_MICROS_COL, \"int64_col\"))" +
              "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
              "NUM_REPLICAS 1 " +
              "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);

      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE_24\" " +
              "AS SELECT STRING_COL, UNIXTIME_MICROS_COL, COUNT(INT32_COL), SUM(INT8_COL) " +
              "FROM \"MY_TABLE_24\" " +
              "GROUP BY STRING_COL, UNIXTIME_MICROS_COL";
      conn.createStatement().execute(ddl2);
      // validate the table can be queried
      ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM \"my_schema.MY_CUBE_24\"");
      assertFalse(rs.next());
    }

    KuduClient client = testHarness.getClient();
    KuduTable kuduTable = client.openTable("my_schema.MY_CUBE_24");

    // validate hash partitioning
    List<PartitionSchema.HashBucketSchema> hashBucketSchemas =
            kuduTable.getPartitionSchema().getHashBucketSchemas();
    assertEquals("Unexpected number of hash partitions", 1, hashBucketSchemas.size());
    assertEquals("Unexpected number of hash buckets", 17, hashBucketSchemas.get(0).getNumBuckets());
    assertEquals("Unexpected hash partition columns", Lists.newArrayList(0),
            hashBucketSchemas.get(0).getColumnIds());

    // validate replicas
    assertEquals("Unexpected number of replicas", 1, kuduTable.getNumReplicas());

    // validate table options
    Map<String, String> expectedConfig = ImmutableMap.<String, String>builder().
            put("kudu.table.history_max_age_sec", "7200").
            build();
    assertEquals("Unexpected extra configs", expectedConfig, kuduTable.getExtraConfig());

    // validate primary key
    assertEquals(2, kuduTable.getSchema().getPrimaryKeyColumnCount());
    List<ColumnSchema> columnSchemas = kuduTable.getSchema().getPrimaryKeyColumns();
    ColumnSchema idColSchema = columnSchemas.get(0);
    assertNotNull(idColSchema);
    assertEquals("STRING_COL", idColSchema.getName());
    assertEquals(ColumnSchema.Encoding.PREFIX_ENCODING, idColSchema.getEncoding());
    assertEquals(ColumnSchema.CompressionAlgorithm.LZ4, idColSchema.getCompressionAlgorithm());
    assertEquals(5000, idColSchema.getDesiredBlockSize());
    ColumnSchema timestampCol = columnSchemas.get(1);
    assertEquals("UNIXTIME_MICROS_COL", timestampCol.getName());
    assertEquals("this column is the timestamp", timestampCol.getComment());

    //validate column attributes
    Schema schema = kuduTable.getSchema();
    validateColumnSchema(schema, "STRING_COL",  Type.STRING, false, "abc");
    validateColumnSchema(schema, "UNIXTIME_MICROS_COL",  Type.UNIXTIME_MICROS, false, 1234567890l);
    validateColumnSchema(schema, "COUNT_INT32_COL",  Type.INT64, false, null);
    validateColumnSchema(schema, "SUM_INT8_COL",  Type.INT8, false, null);
  }

  @Test
  public void testCreateMaterializedViewGroupByColumnsNotInSelect() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {

      String ddl = "CREATE TABLE \"MY_TABLE_14\" (" +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
              "UNIXTIME_MICROS_COL TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
              "is the timestamp', " +
              "\"int64_col\" BIGINT DEFAULT 1234567890, " +
              "INT8_COL TINYINT not null DEFAULT -128," +
              "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
              "INT32_COL INTEGER not null DEFAULT -2147483648, " +
              "BINARY_COL VARBINARY DEFAULT x'AB'," +
              "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
              "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
              "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
              "PRIMARY KEY (STRING_COL, UNIXTIME_MICROS_COL, \"int64_col\"))" +
              "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
              "NUM_REPLICAS 1 " +
              "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);

      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE_14\" " +
              "AS SELECT COUNT(INT32_COL) " +
              "FROM \"MY_TABLE_14\" " +
              "GROUP BY STRING_COL, UNIXTIME_MICROS_COL";
      conn.createStatement().execute(ddl2);
      // validate the table can be queried
      ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM \"my_schema.MY_CUBE_14\"");
      assertFalse(rs.next());
    }

    KuduClient client = testHarness.getClient();
    KuduTable kuduTable = client.openTable("my_schema.MY_CUBE");

    // validate hash partitioning
    List<PartitionSchema.HashBucketSchema> hashBucketSchemas =
            kuduTable.getPartitionSchema().getHashBucketSchemas();
    assertEquals("Unexpected number of hash partitions", 1, hashBucketSchemas.size());
    assertEquals("Unexpected number of hash buckets", 17, hashBucketSchemas.get(0).getNumBuckets());
    assertEquals("Unexpected hash partition columns", Lists.newArrayList(0),
            hashBucketSchemas.get(0).getColumnIds());

    // validate replicas
    assertEquals("Unexpected number of replicas", 1, kuduTable.getNumReplicas());

    // validate table options
    Map<String, String> expectedConfig = ImmutableMap.<String, String>builder().
            put("kudu.table.history_max_age_sec", "7200").
            build();
    assertEquals("Unexpected extra configs", expectedConfig, kuduTable.getExtraConfig());

    // validate primary key
    assertEquals(2, kuduTable.getSchema().getPrimaryKeyColumnCount());
    List<ColumnSchema> columnSchemas = kuduTable.getSchema().getPrimaryKeyColumns();
    ColumnSchema idColSchema = columnSchemas.get(0);
    assertNotNull(idColSchema);
    assertEquals("STRING_COL", idColSchema.getName());
    assertEquals(ColumnSchema.Encoding.PREFIX_ENCODING, idColSchema.getEncoding());
    assertEquals(ColumnSchema.CompressionAlgorithm.LZ4, idColSchema.getCompressionAlgorithm());
    assertEquals(5000, idColSchema.getDesiredBlockSize());
    ColumnSchema timestampCol = columnSchemas.get(1);
    assertEquals("UNIXTIME_MICROS_COL", timestampCol.getName());
    assertEquals("this column is the timestamp", timestampCol.getComment());

    //validate column attributes
    Schema schema = kuduTable.getSchema();
    validateColumnSchema(schema, "STRING_COL",  Type.STRING, false, "abc");
    validateColumnSchema(schema, "UNIXTIME_MICROS_COL",  Type.UNIXTIME_MICROS, false, 1234567890l);
    validateColumnSchema(schema, "COUNT_INT32_COL",  Type.INT64, false, null);
  }

  @Test
  public void testMaterializedViewTableDoesNotExists() throws SQLException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {

      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE_11\" " +
              "AS SELECT STRING_COL, UNIXTIME_MICROS_COL, COUNT(INT32_COL) AS CNT " +
              "FROM \"my_schema.MY_TABLE_11\" " +
              "GROUP BY STRING_COL, UNIXTIME_MICROS_COL";
      // create the cube
      try {
        conn.createStatement().execute(ddl2);
        fail("Creating a cube using a table that does not exist should fail");
      } catch (SQLException e) {
      }
    }
  }

  @Test
  public void testMaterializedViewIfNotExists() throws SQLException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      // create table
      String ddl = "CREATE TABLE \"my_schema.MY_TABLE_1\" (" +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
              "UNIXTIME_MICROS_COL TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
              "is the timestamp', " +
              "\"int64_col\" BIGINT DEFAULT 1234567890, " +
              "INT8_COL TINYINT not null DEFAULT -128," +
              "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
              "INT32_COL INTEGER not null DEFAULT -2147483648, " +
              "BINARY_COL VARBINARY DEFAULT x'AB'," +
              "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
              "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
              "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
              "PRIMARY KEY (STRING_COL, UNIXTIME_MICROS_COL, \"int64_col\"))" +
              "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
              "NUM_REPLICAS 1 " +
              "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);

      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE_1\" " +
              "AS SELECT STRING_COL, UNIXTIME_MICROS_COL, COUNT(INT32_COL) " +
              "FROM \"my_schema.MY_TABLE_1\" " +
              "GROUP BY STRING_COL, UNIXTIME_MICROS_COL";
      conn.createStatement().execute(ddl2);
      // validate the table can be queried
      ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM \"my_schema.MY_CUBE_1\"");
      assertFalse(rs.next());


      // create the cube again
      try {
        conn.createStatement().execute(ddl2);
        fail("Creating a cube that already exists should fail");
      } catch (SQLException e) {
      }

      String ddl3 = "CREATE MATERIALIZED VIEW IF NOT EXISTS \"my_schema.MY_CUBE_1\" " +
              "AS SELECT STRING_COL, UNIXTIME_MICROS_COL, COUNT(INT32_COL) AS CNT " +
              "FROM \"my_schema.MY_TABLE_1\" " +
              "GROUP BY STRING_COL, UNIXTIME_MICROS_COL";
      // use the IF NOT EXISTS clause and verify an exception is not thrown
      // while trying to create the cube again
      try {
        conn.createStatement().execute(ddl3);
      } catch (SQLException e) {
        fail("Creating a cube that already exists using IF NOT EXISTS should not throw an " +
                "exception");
      }
    }
  }

  @Test
  public void testCreateMaterializedViewNoQuery() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {

      String ddl = "CREATE TABLE \"my_schema.MY_TABLE_10\" (" +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
              "UNIXTIME_MICROS_COL TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
              "is the timestamp', " +
              "\"int64_col\" BIGINT DEFAULT 1234567890, " +
              "INT8_COL TINYINT not null DEFAULT -128," +
              "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
              "INT32_COL INTEGER not null DEFAULT -2147483648, " +
              "BINARY_COL VARBINARY DEFAULT x'AB'," +
              "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
              "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
              "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
              "PRIMARY KEY (STRING_COL, UNIXTIME_MICROS_COL, \"int64_col\"))" +
              "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
              "NUM_REPLICAS 1 " +
              "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);

      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE_10\" ";
      try {
        conn.createStatement().execute(ddl2);
        fail("Creating a cube without query should fail");
      }catch (SQLException e) {
      }
    }
  }

  @Test
  public void testCreateMaterializedViewNoSelectList() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {

      String ddl = "CREATE TABLE \"my_schema.MY_TABLE_3\" (" +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
              "UNIXTIME_MICROS_COL TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
              "is the timestamp', " +
              "\"int64_col\" BIGINT DEFAULT 1234567890, " +
              "INT8_COL TINYINT not null DEFAULT -128," +
              "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
              "INT32_COL INTEGER not null DEFAULT -2147483648, " +
              "BINARY_COL VARBINARY DEFAULT x'AB'," +
              "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
              "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
              "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
              "PRIMARY KEY (STRING_COL, UNIXTIME_MICROS_COL, \"int64_col\"))" +
              "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
              "NUM_REPLICAS 1 " +
              "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);

      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE_3\" " +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, ";
      try {
        conn.createStatement().execute(ddl2);
        fail("Creating a cube without select list should fail");
      }catch (SQLException e) {
      }
    }
  }

  @Test
  public void testCreateMaterializedViewNoGroupby() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {

      String ddl = "CREATE TABLE \"my_schema.MY_TABLE_5\" (" +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
              "UNIXTIME_MICROS_COL TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
              "is the timestamp', " +
              "\"int64_col\" BIGINT DEFAULT 1234567890, " +
              "INT8_COL TINYINT not null DEFAULT -128," +
              "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
              "INT32_COL INTEGER not null DEFAULT -2147483648, " +
              "BINARY_COL VARBINARY DEFAULT x'AB'," +
              "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
              "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
              "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
              "PRIMARY KEY (STRING_COL, UNIXTIME_MICROS_COL, \"int64_col\"))" +
              "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
              "NUM_REPLICAS 1 " +
              "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);

      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE_5\" " +
              "AS SELECT STRING_COL, UNIXTIME_MICROS_COL, SUM(INT32_COL) " +
              "FROM \"my_schema.MY_TABLE_5\" ";
      try {
        conn.createStatement().execute(ddl2);
        fail("Creating a cube without group by clause should fail");
      }catch (SQLException e) {
      }
    }
  }

  @Test
  public void testCreateMaterializedViewAggAs() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {

      String ddl = "CREATE TABLE \"my_schema.MY_TABLE_6\" (" +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
              "UNIXTIME_MICROS_COL TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
              "is the timestamp', " +
              "\"int64_col\" BIGINT DEFAULT 1234567890, " +
              "INT8_COL TINYINT not null DEFAULT -128," +
              "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
              "INT32_COL INTEGER not null DEFAULT -2147483648, " +
              "BINARY_COL VARBINARY DEFAULT x'AB'," +
              "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
              "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
              "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
              "PRIMARY KEY (STRING_COL, UNIXTIME_MICROS_COL, \"int64_col\"))" +
              "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
              "NUM_REPLICAS 1 " +
              "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);

      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE_6\" " +
              "AS SELECT STRING_COL, UNIXTIME_MICROS_COL, SUM(INT32_COL) AS X " +
              "FROM \"my_schema.MY_TABLE_6\" " +
              "GROUP BY STRING_COL, UNIXTIME_MICROS_COL";
      try {
        conn.createStatement().execute(ddl2);
        fail("Creating a cube with aliases for column names should fail");
      }catch (SQLException e) {
      }
    }
  }

  @Test
  public void testCreateMaterializedViewAggMAX() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {

      String ddl = "CREATE TABLE \"my_schema.MY_TABLE_26\" (" +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
              "UNIXTIME_MICROS_COL TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
              "is the timestamp', " +
              "\"int64_col\" BIGINT DEFAULT 1234567890, " +
              "INT8_COL TINYINT not null DEFAULT -128," +
              "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
              "INT32_COL INTEGER not null DEFAULT -2147483648, " +
              "BINARY_COL VARBINARY DEFAULT x'AB'," +
              "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
              "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
              "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
              "PRIMARY KEY (STRING_COL, UNIXTIME_MICROS_COL, \"int64_col\"))" +
              "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
              "NUM_REPLICAS 1 " +
              "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);

      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE_26\" " +
              "AS SELECT STRING_COL, UNIXTIME_MICROS_COL, MAX(INT32_COL) " +
              "FROM \"my_schema.MY_TABLE_26\" " +
              "GROUP BY STRING_COL, UNIXTIME_MICROS_COL";
      try {
        conn.createStatement().execute(ddl2);
        fail("Aggregate operator not supported.");
      }catch (SQLException e) {
      }
    }
  }

  @Test
  public void testCreateMaterializedViewAgg() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {

      String ddl = "CREATE TABLE \"my_schema.MY_TABLE_8\" (" +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
              "UNIXTIME_MICROS_COL TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
              "is the timestamp', " +
              "\"int64_col\" BIGINT DEFAULT 1234567890, " +
              "INT8_COL TINYINT not null DEFAULT -128," +
              "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
              "INT32_COL INTEGER not null DEFAULT -2147483648, " +
              "BINARY_COL VARBINARY DEFAULT x'AB'," +
              "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
              "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
              "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
              "PRIMARY KEY (STRING_COL, UNIXTIME_MICROS_COL, \"int64_col\"))" +
              "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
              "NUM_REPLICAS 1 " +
              "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);

      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE_8\" " +
              "AS SELECT STRING_COL, UNIXTIME_MICROS_COL, SUM(INT32_COL) " +
              "FROM \"my_schema.MY_TABLE_8\" " +
              "GROUP BY STRING_COL, UNIXTIME_MICROS_COL";
      conn.createStatement().execute(ddl2);
      // validate the table can be queried
      ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM \"my_schema.MY_CUBE_8\"");
      assertFalse(rs.next());
    }

    KuduClient client = testHarness.getClient();
    KuduTable kuduTable = client.openTable("my_schema.MY_CUBE_8");

    // validate hash partitioning
    List<PartitionSchema.HashBucketSchema> hashBucketSchemas =
            kuduTable.getPartitionSchema().getHashBucketSchemas();
    assertEquals("Unexpected number of hash partitions", 1, hashBucketSchemas.size());
    assertEquals("Unexpected number of hash buckets", 17, hashBucketSchemas.get(0).getNumBuckets());
    assertEquals("Unexpected hash partition columns", Lists.newArrayList(0),
            hashBucketSchemas.get(0).getColumnIds());

    // validate replicas
    assertEquals("Unexpected number of replicas", 1, kuduTable.getNumReplicas());

    // validate table options
    Map<String, String> expectedConfig = ImmutableMap.<String, String>builder().
            put("kudu.table.history_max_age_sec", "7200").
            build();
    assertEquals("Unexpected extra configs", expectedConfig, kuduTable.getExtraConfig());

    // validate primary key
    assertEquals(2, kuduTable.getSchema().getPrimaryKeyColumnCount());
    List<ColumnSchema> columnSchemas = kuduTable.getSchema().getPrimaryKeyColumns();
    ColumnSchema idColSchema = columnSchemas.get(0);
    assertNotNull(idColSchema);
    assertEquals("STRING_COL", idColSchema.getName());
    assertEquals(ColumnSchema.Encoding.PREFIX_ENCODING, idColSchema.getEncoding());
    assertEquals(ColumnSchema.CompressionAlgorithm.LZ4, idColSchema.getCompressionAlgorithm());
    assertEquals(5000, idColSchema.getDesiredBlockSize());
    ColumnSchema timestampCol = columnSchemas.get(1);
    assertEquals("UNIXTIME_MICROS_COL", timestampCol.getName());
    assertEquals("this column is the timestamp", timestampCol.getComment());

    //validate column attributes
    Schema schema = kuduTable.getSchema();
    validateColumnSchema(schema, "STRING_COL",  Type.STRING, false, "abc");
    validateColumnSchema(schema, "UNIXTIME_MICROS_COL",  Type.UNIXTIME_MICROS, false, 1234567890l);
    validateColumnSchema(schema, "SUM_INT32_COL",  Type.INT32, false, null);
  }

  @Test
  public void testCreateMaterializedViewSelectStar() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
            testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {

      String ddl = "CREATE TABLE \"my_schema.MY_TABLE_7\" (" +
              "STRING_COL VARCHAR " +
              "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
              "\"unixtime_micros_col\" TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
              "is the timestamp', " +
              "\"int64_col\" BIGINT DEFAULT 1234567890, " +
              "INT8_COL TINYINT not null DEFAULT -128," +
              "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
              "INT32_COL INTEGER not null DEFAULT -2147483648, " +
              "BINARY_COL VARBINARY DEFAULT x'AB'," +
              "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
              "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
              "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
              "PRIMARY KEY (STRING_COL, \"unixtime_micros_col\", \"int64_col\"))" +
              "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
              "NUM_REPLICAS 1 " +
              "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);


      String ddl2 = "CREATE MATERIALIZED VIEW \"my_schema.MY_CUBE_7\" " +
                     "AS SELECT * " +
              "FROM \"my_schema.MY_TABLE_7\" " +
              "GROUP BY X";
      try {
        conn.createStatement().execute(ddl2);
        fail("Creating a cube without using select * should fail");
      }catch (SQLException e) {
      }
    }
  }

  @Test
  public void testCreateTable() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
      testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      String ddl = "CREATE TABLE \"my_schema.MY_TABLE\" (" +
        "STRING_COL VARCHAR " +
        "COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000, " +
        "\"unixtime_micros_col\" TIMESTAMP DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column " +
        "is the timestamp', " +
        "\"int64_col\" BIGINT DEFAULT 1234567890, " +
        "INT8_COL TINYINT not null DEFAULT -128," +
        "\"int16_col\" SMALLINT not null DEFAULT -32768, " +
        "INT32_COL INTEGER not null DEFAULT -2147483648, " +
        "BINARY_COL VARBINARY DEFAULT x'AB'," +
        "FLOAT_COL FLOAT DEFAULT 0.0123456789," +
        "\"double_col\" DOUBLE DEFAULT 0.0123456789," +
        "DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456, " +
        "PRIMARY KEY (STRING_COL, \"unixtime_micros_col\", \"int64_col\"))" +
        "PARTITION BY HASH (STRING_COL) PARTITIONS 17 " +
        "NUM_REPLICAS 1 " +
        "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')";
      conn.createStatement().execute(ddl);
      // validate the table can be queried
      ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM \"my_schema.MY_TABLE\"");
      assertFalse(rs.next());
    }

    KuduClient client = testHarness.getClient();
    KuduTable kuduTable = client.openTable("my_schema.MY_TABLE");

    // validate hash partitioning
    List<PartitionSchema.HashBucketSchema> hashBucketSchemas =
      kuduTable.getPartitionSchema().getHashBucketSchemas();
    assertEquals("Unexpected number of hash partitions", 1, hashBucketSchemas.size());
    assertEquals("Unexpected number of hash buckets", 17, hashBucketSchemas.get(0).getNumBuckets());
    assertEquals("Unexpected hash partition columns", Lists.newArrayList(0),
      hashBucketSchemas.get(0).getColumnIds());

    // validate replicas
    assertEquals("Unexpected number of replicas", 1, kuduTable.getNumReplicas());

    // validate table options
    Map<String, String> expectedConfig = ImmutableMap.<String, String>builder().
      put("kudu.table.history_max_age_sec", "7200").
      build();
    assertEquals("Unexpected extra configs", expectedConfig, kuduTable.getExtraConfig());

    // validate primary key
    assertEquals(3, kuduTable.getSchema().getPrimaryKeyColumnCount());
    List<ColumnSchema> columnSchemas = kuduTable.getSchema().getPrimaryKeyColumns();
    ColumnSchema idColSchema = columnSchemas.get(0);
    assertNotNull(idColSchema);
    assertEquals("STRING_COL", idColSchema.getName());
    assertEquals(ColumnSchema.Encoding.PREFIX_ENCODING, idColSchema.getEncoding());
    assertEquals(ColumnSchema.CompressionAlgorithm.LZ4, idColSchema.getCompressionAlgorithm());
    assertEquals(5000, idColSchema.getDesiredBlockSize());
    ColumnSchema timestampCol = columnSchemas.get(1);
    assertEquals("unixtime_micros_col", timestampCol.getName());
    assertEquals("this column is the timestamp", timestampCol.getComment());
    ColumnSchema int64Col = columnSchemas.get(2);
    assertEquals("int64_col", int64Col.getName());

    //validate column attributes
    Schema schema = kuduTable.getSchema();
    validateColumnSchema(schema, "STRING_COL",  Type.STRING, false, "abc");
    validateColumnSchema(schema, "unixtime_micros_col",  Type.UNIXTIME_MICROS, false, 1234567890l);
    validateColumnSchema(schema, "int64_col",  Type.INT64, false, 1234567890l);
    validateColumnSchema(schema, "INT8_COL",  Type.INT8, false, (byte)-128);
    validateColumnSchema(schema, "int16_col",  Type.INT16, false, (short)-32768);
    validateColumnSchema(schema, "INT32_COL",  Type.INT32, false, -2147483648);
    validateColumnSchema(schema, "BINARY_COL",  Type.BINARY, true, new byte[]{-85});
    validateColumnSchema(schema, "FLOAT_COL",  Type.FLOAT, true, 0.0123456789f);
    validateColumnSchema(schema, "double_col",  Type.DOUBLE, true, 0.0123456789d);
    validateColumnSchema(schema, "DECIMAL_COL",  Type.DECIMAL, true,
      new BigDecimal("1234567890.123456"));

  }

  private void validateColumnSchema(Schema schema, String columnName, Type expectedType,
                                    boolean nullable, Object defaultValue) {
    ColumnSchema columnSchema = schema.getColumn(columnName);
    assertEquals("Unexpected type for column " + columnName, expectedType, columnSchema.getType());
    assertEquals("Unexpected nullabilty for column " + columnName, nullable, columnSchema.isNullable());
    if (expectedType == Type.BINARY) {
     assertArrayEquals("Unexpected default value for column " + columnName, (byte[])defaultValue,
       (byte[])columnSchema.getDefaultValue());
    }
    else {
      assertEquals("Unexpected default value for column " + columnName, defaultValue,
        columnSchema.getDefaultValue());
    }
  }

  @Test
  public void testCreateTableWithPKColumnAttribute() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
      testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      conn.createStatement().execute("CREATE TABLE TABLE1 (PK_COL VARCHAR " +
        "PRIMARY KEY, NON_PK_COL VARCHAR) PARTITION BY HASH (PK_COL) PARTITIONS 2");

      ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TABLE1");
      assertFalse(rs.next());

      KuduClient client = testHarness.getClient();
      KuduTable kuduTable = client.openTable("TABLE1");

      assertEquals(1, kuduTable.getSchema().getPrimaryKeyColumnCount());
      Schema pkColumnSchema = kuduTable.getSchema().getRowKeyProjection();
      ColumnSchema colSchema = pkColumnSchema.getColumn("PK_COL");
      assertNotNull(colSchema);

      // using both the PRIMARY KEY column attribute and PRIMARY KEY constraint clause should fail
      try {
      conn.createStatement().execute("CREATE TABLE TABLE2 (PK_COL VARCHAR " +
        "PRIMARY KEY, NON_PK_COL VARCHAR, PRIMARY KEY (PK_COL)) PARTITION BY HASH (PK_COL) " +
        "PARTITIONS 2");
        fail("Using both the PRIMARY KEY column attribute and PRIMARY KEY constraint clause " +
          "should fail");
      } catch (SQLException e) {
      }
    }
  }

  @Test
  public void testIfNotExists() throws SQLException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
      testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      // create table
      conn.createStatement().execute("CREATE TABLE TABLE3 (PK_COL VARCHAR " +
        "PRIMARY KEY, NON_PK_COL VARCHAR) PARTITION BY HASH (PK_COL) PARTITIONS 2");

      ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TABLE3");
      assertFalse(rs.next());

      // create the table again
      try {
        conn.createStatement().execute("CREATE TABLE TABLE3 (A VARCHAR " +
          "PRIMARY KEY, B VARCHAR) PARTITION BY HASH (A) PARTITIONS 2");
        fail("Creating a table that already exists should fail");
      } catch (SQLException e) {
      }

      // use the IF NOT EXISTS clause and verify an exception is not thrown
      // while trying to create the table again
      try {
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS TABLE3 (A" +
          " VARCHAR PRIMARY KEY, B VARCHAR) PARTITION BY HASH (A) PARTITIONS 2");
      } catch (SQLException e) {
        fail("Creating a table that already exists using IF NOT EXISTS should not throw an " +
          "exception");
      }
    }
  }

  @Test
  public void testPKConstraintColumnOrder() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
      testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      // create table
      conn.createStatement().execute("CREATE TABLE TABLE4 (" +
        "PK_COL1 VARCHAR, " +
        "PK_COL2 INTEGER, " +
        "NON_PK_COL VARCHAR, " +
        "PRIMARY KEY(PK_COL2, PK_COL1)) " +
        "PARTITION BY HASH (PK_COL1) PARTITIONS 2");
      ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TABLE4");
      assertFalse(rs.next());
    }

    KuduClient client = testHarness.getClient();
    KuduTable kuduTable = client.openTable("TABLE4");

    // validate primary key columns are in the correct order even thoough the order of column
    // definitions do not match
    assertEquals(2, kuduTable.getSchema().getPrimaryKeyColumnCount());
    List<ColumnSchema> schemas = kuduTable.getSchema().getPrimaryKeyColumns();
    assertEquals("PK_COL2", schemas.get(0).getName());
    assertEquals("PK_COL1", schemas.get(1).getName());
  }

  @Test(expected = SQLException.class)
  public void testMultipelRowTimestampColumns() throws SQLException, KuduException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
      testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      // create table
      conn.createStatement().execute("CREATE TABLE TABLE4 (" +
        "PK_COL1 VARCHAR, " +
        "PK_COL2 TIMESTAMP ROW_TIMESTAMP, " +
        "PK_COL3 TIMESTAMP ROW_TIMESTAMP, " +
        "PRIMARY KEY(PK_COL1, PK_COL2, PK_COL3)) " +
        "PARTITION BY HASH (PK_COL1) PARTITIONS 2");
      fail("Only one row timestamp column can be defined");
    }
  }

  private void helpTestDefaultValueTypeMismatch(String type, String defaultValue){
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
      testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      String ddlFormat = "CREATE TABLE TABLE5 (PKCOL %s PRIMARY KEY DEFAULT %s) " +
        "PARTITION BY HASH (PKCOL) PARTITIONS 2";
      String ddl = String.format(ddlFormat, type, defaultValue);
      conn.createStatement().execute(ddl);
      fail();
    }
    catch (SQLException e){
    }
  }

  @Test
  public void testDefaultValueTypeMismatch() {
    helpTestDefaultValueTypeMismatch("VARCHAR", "123");
    helpTestDefaultValueTypeMismatch("TIMESTAMP", "'abc'");
    helpTestDefaultValueTypeMismatch("BIGINT", "'abc'");
    helpTestDefaultValueTypeMismatch("TINYINT", "'abc'");
    helpTestDefaultValueTypeMismatch("SMALLINT", "'abc'");
    helpTestDefaultValueTypeMismatch("INTEGER", "'abc'");
    helpTestDefaultValueTypeMismatch("VARBINARY", "'abc'");
    helpTestDefaultValueTypeMismatch("FLOAT", "'abc'");
    helpTestDefaultValueTypeMismatch("DOUBLE", "'abc'");
    helpTestDefaultValueTypeMismatch("DECIMAL(22, 6)", "'abc'");
  }

  @Test
  public void testAddingRangePartition() throws Exception {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
      testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      String ddl = "CREATE TABLE TABLE5 (" +
        "STRING_COL VARCHAR, " +
        "TIMESTAMP_COL TIMESTAMP ROW_TIMESTAMP, " +
        "INT_COL INTEGER, " +
        "PRIMARY KEY (STRING_COL, TIMESTAMP_COL, INT_COL))" +
        "PARTITION BY HASH (STRING_COL) PARTITIONS 17";
      conn.createStatement().execute(ddl);
      // TODO why isn't this working
      // validate the table can be queried
      ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TABLE5");
      assertFalse(rs.next());
    }

    KuduClient client = testHarness.getClient();
    KuduTable kuduTable = client.openTable("TABLE5");

    AlterTableOptions options = new AlterTableOptions();
    PartialRow row1 = kuduTable.getSchema().newPartialRow();
    long currentTime = System.currentTimeMillis();
    row1.addTimestamp("TIMESTAMP_COL", new Timestamp(currentTime));
    PartialRow row2 = kuduTable.getSchema().newPartialRow();
    row2.addTimestamp("TIMESTAMP_COL",
      new Timestamp(currentTime + TimeUnit.DAYS.toMillis(1)));
    options.addRangePartition(row1, row2);

    client.alterTable("TABLE5", options);
  }

  @Test(expected = SQLException.class)
  public void testCreateView() throws SQLException {
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED,
      testHarness.getMasterAddressesAsString());
    try (Connection conn = DriverManager.getConnection(url)) {
      // create table
      conn.createStatement().execute("create view s.v as select * from s.t");
      fail("create view is not supported");
    }
  }

}
