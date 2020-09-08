package com.twilio.raas.sql.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.SourceStringReader;
import org.junit.Test;

import java.io.Reader;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;

public class KuduParserTest {

  private static final SqlWriterConfig SQL_WRITER_CONFIG = SqlPrettyWriter.config();

  private SqlParser getSqlParser(Reader source, UnaryOperator<SqlParser.ConfigBuilder> transform) {
    SqlParser.ConfigBuilder configBuilder = SqlParser.configBuilder()
      .setParserFactory(KuduSqlParserImpl.FACTORY);
    SqlParser.Config config = transform.apply(configBuilder).build();
    return SqlParser.create(source, config);
  }

  private SqlNode parseStmtAndHandleEx(String sql, UnaryOperator<SqlParser.ConfigBuilder> transform) {
    SqlParser parser = getSqlParser(new SourceStringReader(sql), transform);

    try {
      SqlNode sqlNode = parser.parseStmt();
      return sqlNode;
    } catch (SqlParseException e) {
      throw new RuntimeException("Error while parsing SQL: " + sql, e);
    }
  }

  public void check(String sql, String expected) {
    SqlNode sqlNode = parseStmtAndHandleEx(sql, UnaryOperator.identity());
    String actual = sqlNode.toSqlString((c) -> SQL_WRITER_CONFIG).getSql();
    assertEquals(expected, actual);
  }

  @Test
  public void testCreateTable() {
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
      "TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200)";

    String expectedUnparsedDdl = "CREATE TABLE \"my_schema.MY_TABLE\" (\"STRING_COL\"  VARCHAR  " +
      "COLUMN_ENCODING  'PREFIX_ENCODING'  COMPRESSION  'LZ4'  DEFAULT  'abc'  BLOCK_SIZE  5000, " +
      "\"unixtime_micros_col\"  TIMESTAMP  DEFAULT  1234567890  ROW_TIMESTAMP  COMMENT  'this " +
      "column is the timestamp', \"int64_col\"  BIGINT  DEFAULT  1234567890, \"INT8_COL\"  " +
      "TINYINT  NOT NULL  DEFAULT  -128, \"int16_col\"  SMALLINT  NOT NULL  DEFAULT  -32768, " +
      "\"INT32_COL\"  INTEGER  NOT NULL  DEFAULT  -2147483648, \"BINARY_COL\"  VARBINARY  DEFAULT" +
      "  X'AB', \"FLOAT_COL\"  FLOAT  DEFAULT  0.0123456789, \"double_col\"  DOUBLE  DEFAULT  0" +
      ".0123456789, \"DECIMAL_COL\"  DECIMAL (22,6) DEFAULT  1234567890.123456 , PRIMARY KEY( " +
      "\"STRING_COL\", \"unixtime_micros_col\", \"int64_col\")) PARTITION BY HASH( STRING_COL) " +
      "PARTITIONS  17 TBLPROPERTIES ( 'kudu.table.history_max_age_sec'=7200)";

    check(ddl,expectedUnparsedDdl);
  }


}
