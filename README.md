# calcite-kudu

An Adapter for [Apache Calcite](https://calcite.apache.org/) that allows a Java service to query [Apache Kudu](https://kudu.apache.org/).
Apache Calcite provides a SQL parser and customizable optimizer that is used in several large products within the Apache Foundation,
calcite-Kudu leverages it to execute queries against Kudu -- a fast analytics database.

## Usage

### As a Command Line Client
Calcite Kudu ships a shaded jar file that can be used as a SQL Client for an existing Kudu Cluster. Maven can be used to download the jar and then executed through Java Virtual Machine

``` bash
$ mvn org.apache.maven.plugins:maven-dependency-plugin:get \
	-Dartifact=com.twilio:kudu-sql-cli:1.0.17
$ java -jar ~/.m2/repository/com/twilio/kudu-sql-cli/1.0.17/kudu-sql-cli-1.0.17.jar -c kudu-leader1,kudu-leader2,kudu-leader3
```

This will work from a machine able to communicate with both Kudu Leaders and Kudu TServers. To test this out locally follow the [README](./cli/README.md) in the cli directory.


### As a Library
Add the dependency to your project:

``` xml
<dependency>
	<groupId>com.twilio</groupId>
	<artifactId>kudu-sql-adapter</artifactId>
	<version>{VERSION}</version>
</dependency>
```

#### JDBC Support
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

## DDL Grammar

### CREATE TABLE
Creates a new kudu table and specifies its properties. The table must define a column of TIMESTAMP data type with
the ROW_TIMESTAMP attribute that will be used for range partitioning. The statement does support specifying range partitions,
these have to be created using kudu tools.
```
CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
(col_name data_type
	[kudu_column_attribute ...]
	[, ...]
	[PRIMARY KEY (col_name[, ...])]
)
[PARTITION BY HASH [ (pk_col [, ...]) ] PARTITIONS n]
[TBLPROPERTIES ('key1'='value1', 'key2'='value2', ...)]

kudu column attributes:
[PRIMARY KEY]
| [ASC | DESC]
| [NOT NULL]
| [COLUMN_ENCODING codec]
| [COMPRESSION algorithm]
| [DEFAULT constant]
| [BLOCK_SIZE number]
| [ROW_TIMESTAMP]
| [COMMENT 'col_comment']

data_type:
	TINYINT
| SMALLINT
| INT
| BIGINT
| BOOLEAN
| FLOAT
| DOUBLE
| DECIMAL
| STRING
| CHAR
| VARCHAR
| TIMESTAMP
```

### ALTER TABLE
Can be used to add or drop columns from a table.
```
ALTER TABLE name
ADD [IF NOT EXISTS]
(col_name data_type
	[kudu_column_attribute ...]
	[, ...]
)

ALTER TABLE name
DROP [IF EXISTS]
(col_name [, ...])
```

### CREATE MATERIALIZED VIEW
Used to register a rollup aggregate view that can be used for queries.
The select query should contain a group by with a timestamp column that is being truncated.
```
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db_name.]materialized_view_name
AS query

query:
SELECT { aggFunc [, aggFunc ]* }
	FROM tableExpression
	[ WHERE booleanExpression ]
	[ GROUP BY { groupItem [, groupItem ]* } ]

aggFunc:
	COUNT | SUM

groupItem:
	col_name | FLOOR(col_name TO interval)
```

## Descending Sort Implementation
Apache Kudu **doesn't** support `DESCENDING` sort keys. To provide this functionally, we decided to write the data in a particular way:

``` java
	public static final Instant EPOCH_DAY_FOR_REVERSE_SORT = Instant.parse("9999-12-31T00:00:00.000000Z");
	public static final Long EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS = EPOCH_DAY_FOR_REVERSE_SORT.toEpochMilli();
	// EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS - ACTUAL-DATE-OF-EVENT = DATE-TO-STORE
	public static final Long EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS = TimestampUtil
		.timestampToMicros(new Timestamp(EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS));
```

Using these constants at write time, our system writes to Kudu a translated date field using this formula `EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS - ACTUAL-DATE-OF-EVENT`. And at read time, our system reverses this logic keeping it transparent to the end user. `Calcite-Kudu` translates timestamps at read time back into their actual values.
This implementation allows `Calcite-Kudu` to leverage Kudu's existing sort logic eliminating the need to sort on the client.

When creating a table using DDL, specify the column as `DESC`. In the following example, the data is stored in descending order of
`event_time`.

``` sql
CREATE TABLE "host_metrics" (
	"host" VARCHAR NOT NULL,
	"metric_name" VARCHAR NOT NULL,
	"event_time" TIMESTAMP NOT NULL ROW_TIMESTAMP,
	"metric_value" DOUBLE NOT NULL,
	PRIMARY KEY ("host", "metric_name", "event_time" DESC))
PARTITION BY HASH ("host") PARTITIONS 2;
```

## Write Support
Data can be written to kudu by using an INSERT statement, but this should only be used for testing.
Materialized views that are defined will be automatically maintained correctly as long as the data for a day is written from a
single client in a single session.
``` sql
INSERT INTO "host_metrics" VALUES ('host1', 'metric1', TIMESTAMP'2020-01-02', 10);
```

## Materialized Views
The adapter allows the querying of rollup aggregate materialized views which can be registered using the
`CREATE MATERIALZIED VIEW` statement. These views have to be maintained by an external system.
While executing a query against a table, a view will be automatically
used if possible. For example the following statement creates a view that stores the sum of the ``metric_value``
grouped by ``metric_name`` for each day.
```sql
CREATE MATERIALIZED VIEW "host_metrics_view" AS
SELECT SUM("metric_value") as "sum_metric_value", COUNT(*) as "count_records"
FROM "host_metrics0" GROUP BY "metric_name", FLOOR("event_time" TO DAY);
```

If we execute the following sql that queries for the sum of `metric_value`` grouped by `metric_name`` for each day
we see that the materialized view is used. Also note that a filter is pushed down to restrict data to the correct time range.
``` sql
SELECT   SUM("metric_value")
FROM     host_metrics0
WHERE    "event_time" >= timestamp '2020-01-01 00:00:00'
AND      "event_time"<'2020-01-02 00:00:00'
GROUP BY "metric_name",
		floor("event_time" TO day);
```
``` sql
KuduToEnumerableRel
KuduProjectRel(EXPR$0=[$2])
	KuduFilterRel(ScanToken 1=[event_time GREATER_EQUAL 1577836800000000, event_time LESS 1577923200000000])
	KuduQuery(table=[[kudu, host_metrics0-host_metrics_view0-Day-Aggregation]])
```

If we execute the following query that returns the total number of metrics that span a time range of one day
and 12 hours we see that a union query is used to query both the table and view with the appropriate time ranges.
``` sql
SELECT count(*)
FROM   host_metrics0
WHERE  "event_time" >= timestamp '2020-01-01 00:00:00'
	AND "event_time" < '2020-01-02 12:00:00';
```
``` sql
EnumerableAggregate(group=[{}], EXPR$0=[$SUM0($0)])
EnumerableUnion(all=[true])
	EnumerableAggregate(group=[{}], EXPR$0=[COUNT()])
	KuduToEnumerableRel
		KuduProjectRel(EVENT_TIME=[$2])
		KuduFilterRel(ScanToken 1=[event_time GREATER_EQUAL 1577923200000000, event_time LESS 1577966400000000])
			KuduQuery(table=[[kudu, host_metrics0]])
	EnumerableAggregate(group=[{}], EXPR$0=[$SUM0($3)])
	KuduToEnumerableRel
		KuduFilterRel(ScanToken 1=[event_time GREATER_EQUAL 1577836800000000, event_time LESS 1577923200000000])
		KuduQuery(table=[[kudu, host_metrics0-host_metrics_view0-Day-Aggregation]])
```
The calcite [documentation](https://calcite.apache.org/docs/materialized_views.html)
describes how materialized views work in detail.
