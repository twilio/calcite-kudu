# calcite-kudu

An Adapter for [Apache Calcite](https://calcite.apache.org/) that allows a Java service to query [Apache Kudu](https://kudu.apache.org/). Apache Calcite is a SQL library used in several large products within Apache Foundation to parse, optimize and execute queries and Kudu is a fast analytics database.

## Usage
Add the dependency to your project:

``` xml
<dependency>
	<groupId>com.twilio</groupId>
	<artifactId>kudu-sql-adapter</artifactId>
	<version>1.0.13</version>
</dependency>
```

### JDBC Support
The library provides a JDBC connection template that can be used with any JDBC library. To create the JDBC connection string:

``` java
import com.twilio.kudu.sql.JDBCUtil;
import com.twilio.kudu.sql.schema.DefaultKuduSchemaFactory;
// Use JDBCUtil#CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED for INSERT and DDL support
final String jdbcURL = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE,
	DefaultKuduSchemaFactory.class.getName(),
	kuduConnectionString);
```

This JDBC url can be handed off to any of the database pooling libraries like
1. [Apache Commons DPCP](https://commons.apache.org/proper/commons-dbcp/download_dbcp.cgi)
2. [HikariCP](https://github.com/brettwooldridge/HikariCP)
3. [C3P0](https://www.mchange.com/projects/c3p0/)

to execute queries.

## Descending Sort Implementation
Apache Kudu **doesn't** support `DESCENDING` sort keys. To provide this functionally, we decided to write the data in a particular way:

``` java
	public static final Instant EPOCH_DAY_FOR_REVERSE_SORT = Instant.parse("9999-12-31T00:00:00.000000Z");
	public static final Long EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS = EPOCH_DAY_FOR_REVERSE_SORT.toEpochMilli();
	// EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS - ACTUAL-DATE-OF-EVENT = DATE-TO-STORE
	public static final Long EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS = TimestampUtil
		.timestampToMicros(new Timestamp(EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS));
```


Using these constants at write time, our system writes to Kudu a translated date field using this formula `EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS - ACTUAL-DATE-OF-EVENT`. And at read time, our system reverses this logic keeping it transparent to the end user. `Calcite-Kudu` translates timestamps at read time back into their actual values. This implementation allows `Calcite-Kudu` to leverage Kudu's existing sort logic preventing costly sorting behavior in our services.

When creating a Table using DDL, specify the column as `DESC`. In the following example, `UNIXTIME_MICROS_COL` is setup as `DESCENDING` sort.

``` sql
CREATE TABLE "DEMO" (
STRING_COL VARCHAR COLUMN_ENCODING 'PREFIX_ENCODING' COMPRESSION 'LZ4' DEFAULT 'abc' BLOCK_SIZE 5000,
UNIXTIME_MICROS_COL TIMESTAMP DESC DEFAULT 1234567890 ROW_TIMESTAMP COMMENT 'this column is the timestamp',
INT64_COL BIGINT DEFAULT 1234567890,
INT8_COL TINYINT not null DEFAULT -128,
INT16_COL SMALLINT not null DEFAULT -32768,
INT32_COL INTEGER not null DEFAULT -2147483648 COMMENT 'INT32 column',
BINARY_COL VARBINARY DEFAULT x'AB',
FLOAT_COL FLOAT DEFAULT 0.0123456789,
DOUBLE_COL DOUBLE DEFAULT 0.0123456789,
DECIMAL_COL DECIMAL(22, 6) DEFAULT 1234567890.123456,
PRIMARY KEY (STRING_COL, UNIXTIME_MICROS_COL, INT64_COL))
PARTITION BY HASH (STRING_COL) PARTITIONS 17 NUM_REPLICAS 1
TBLPROPERTIES ('kudu.table.history_max_age_sec'=7200, 'invalid.property'='1234')
```
