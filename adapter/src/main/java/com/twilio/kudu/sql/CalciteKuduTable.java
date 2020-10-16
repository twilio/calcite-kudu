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

import com.twilio.kudu.sql.metadata.CubeTableInfo;
import com.twilio.kudu.sql.rules.KuduToEnumerableConverter;
import org.apache.calcite.jdbc.KuduCalciteConnectionImpl;
import org.apache.calcite.jdbc.KuduMetaImpl;
import org.apache.calcite.linq4j.Enumerable;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kudu.client.KuduTable;
import com.google.common.annotations.VisibleForTesting;
import org.apache.kudu.util.TimestampUtil;
import org.apache.kudu.Schema;
import org.apache.kudu.ColumnSchema;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.kudu.client.AsyncKuduClient;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.kudu.client.KuduPredicate;

import java.util.Collections;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code CalciteKuduTable} is responsible for returning rows of Objects back.
 * It is responsible for calling the RPC layer of Kudu reparsing them into
 * expected types for the RelTable used in the request.
 *
 * It requires a {@link KuduTable} to be opened and ready to be used.
 */
public class CalciteKuduTable extends AbstractQueryableTable implements TranslatableTable {
  private static final Logger logger = LoggerFactory.getLogger(CalciteKuduTable.class);

  public static final Instant EPOCH_DAY_FOR_REVERSE_SORT = Instant.parse("9999-12-31T00:00:00.000000Z");
  public static final Long EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS = EPOCH_DAY_FOR_REVERSE_SORT.toEpochMilli();
  public static final Long EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS = TimestampUtil
      .timestampToMicros(new Timestamp(EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS));

  private final static Double DIMENSION_TABLE_ROW_COUNT = 1000.0;
  private final static Double CUBE_TABLE_ROW_COUNT = 2000000.0;
  private final static Double FACT_TABLE_ROW_COUNT = 20000000.0;

  protected final KuduTable kuduTable;
  protected final AsyncKuduClient client;

  // list of column indexes for columns that are stored in descending order
  protected final List<Integer> descendingOrderedColumnIndexes;

  // index of the column that represents event time
  protected final int timestampColumnIndex;
  // list of cube tables (if this is a fact table)
  protected final List<CalciteKuduTable> cubeTables;

  // type of table
  protected final TableType tableType;

  // type of table
  protected final CubeTableInfo.EventTimeAggregationType eventTimeAggregationType;

  /**
   * Create the {@code CalciteKuduTable} for a physical scan over the
   * provided{@link KuduTable}. {@code KuduTable} must exist and be opened.
   *
   * Use {@link com.twilio.kudu.sql.CalciteKuduTableBuilder to build a table
   * instead of this constructor
   *
   * @param kuduTable                    the opened kudu table
   * @param client                       kudu client that is used to write
   *                                     mutations
   * @param descendingOrderColumnIndexes indexes of columns that are stored in
   *                                     descending ordere
   * @param cubeTables                   the list of cube tables (if this is a
   *                                     fact table)
   * @param tableType                    type of this table
   */
  public CalciteKuduTable(final KuduTable kuduTable, final AsyncKuduClient client,
      final List<Integer> descendingOrderColumnIndexes, final int timestampColumnIndex,
      final List<CalciteKuduTable> cubeTables, final TableType tableType,
      final CubeTableInfo.EventTimeAggregationType eventTimeAggregationType) {
    super(Object[].class);
    this.kuduTable = kuduTable;
    this.client = client;
    this.descendingOrderedColumnIndexes = descendingOrderColumnIndexes;
    this.cubeTables = cubeTables;
    this.tableType = tableType;
    this.timestampColumnIndex = timestampColumnIndex;
    this.eventTimeAggregationType = eventTimeAggregationType;
  }

  @Override
  public Statistic getStatistic() {
    final List<ImmutableBitSet> primaryKeys = Collections
        .singletonList(ImmutableBitSet.range(this.kuduTable.getSchema().getPrimaryKeyColumnCount()));
    switch (tableType) {
    case CUBE:
      return Statistics.of(CUBE_TABLE_ROW_COUNT, primaryKeys, Collections.emptyList(),
          // We don't always sort for two reasons:
          // 1. When applying a Filter we want to also sort that doesn't magically happen
          // by
          // setting this as a RelCollation
          // 2. Awhile ago we saw performance degrade with always sorting.
          Collections.emptyList());
    case FACT:
      return Statistics.of(FACT_TABLE_ROW_COUNT, primaryKeys, Collections.emptyList(),
          // We don't always sort for two reasons:
          // 1. When applying a Filter we want to also sort that doesn't magically happen
          // by
          // setting this as a RelCollation
          // 2. Awhile ago we saw performance degrade with always sorting.
          Collections.emptyList());
    case DIMENSION:
      return Statistics.of(DIMENSION_TABLE_ROW_COUNT, primaryKeys, Collections.emptyList(),
          // We don't always sort for two reasons:
          // 1. When applying a Filter we want to also sort that doesn't magically happen
          // by
          // setting this as a RelCollation
          // 2. Awhile ago we saw performance degrade with always sorting.
          Collections.emptyList());
    }
    return super.getStatistic();
  }

  /**
   * Builds a mapping from Kudu Schema into relational schema names
   */
  @Override
  public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
    final Schema kuduSchema = this.getKuduTable().getSchema();
    final RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);

    for (int i = 0; i < kuduSchema.getColumnCount(); i++) {
      final ColumnSchema currentColumn = kuduSchema.getColumnByIndex(i);
      switch (currentColumn.getType()) {
      case INT8:
        builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.TINYINT).nullable(currentColumn.isNullable());
        break;
      case INT16:
        builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.SMALLINT).nullable(currentColumn.isNullable());
        break;
      case INT32:
        builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.INTEGER).nullable(currentColumn.isNullable());
        break;
      case INT64:
        builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.BIGINT).nullable(currentColumn.isNullable());
        break;
      case BINARY:
        builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.VARBINARY).nullable(currentColumn.isNullable());
        break;
      case STRING:
        builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.VARCHAR).nullable(currentColumn.isNullable());
        break;
      case BOOL:
        builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.BOOLEAN).nullable(currentColumn.isNullable());
        break;
      case FLOAT:
        builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.FLOAT).nullable(currentColumn.isNullable());
        break;
      case DOUBLE:
        builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.DOUBLE).nullable(currentColumn.isNullable());
        break;
      case UNIXTIME_MICROS:
        builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.TIMESTAMP).nullable(currentColumn.isNullable());
        break;
      case DECIMAL:
        builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.DECIMAL).nullable(currentColumn.isNullable());
        break;
      }
    }

    return builder.build();
  }

  @Override
  public RelNode toRel(final RelOptTable.ToRelContext context, final RelOptTable relOptTable) {

    final RelOptCluster cluster = context.getCluster();
    return new KuduQuery(cluster, cluster.traitSetOf(KuduRelNode.CONVENTION), relOptTable, this,
        this.getRowType(context.getCluster().getTypeFactory()));
  }

  /**
   * Run the query against the kudu table {@link kuduTable}. {@link KuduPredicate}
   * are the filters to apply to the query, {@link columnIndices} are the columns
   * to return in the response and finally {@link limit} is used to limit the
   * results that come back.
   *
   * @param predicates     each member in the first list represents a single scan.
   * @param columnIndices  the fields ordinals to select out of Kudu
   * @param limit          process the results until limit is reached. If less
   *                       then 0, no limit is enforced
   * @param offset         skip offset number of rows before returning results
   * @param sorted         whether to return rows in sorted order
   * @param groupByLimited indicates if the groupBy method should be counting
   *                       unique keys
   *
   * @return Enumeration on the objects, Fields conform to
   *         {@link CalciteKuduTable#getRowType}.
   */
  public KuduEnumerable executeQuery(final List<List<CalciteKuduPredicate>> predicates,
      final List<Integer> columnIndices, final long limit, final long offset, final boolean sorted,
      final boolean groupByLimited, final KuduScanStats scanStats, final AtomicBoolean cancelFlag,
      final Function1<Object, Object> projection, final Predicate1<Object> filterFunction) {
    return new KuduEnumerable(predicates, columnIndices, this.client, this, limit, offset, sorted, groupByLimited,
        scanStats, cancelFlag, projection, filterFunction);
  }

  @Override
  public <T> Queryable<T> asQueryable(final QueryProvider queryProvider, final SchemaPlus schema,
      final String tableName) {
    return new KuduQueryable<>(queryProvider, schema, this, tableName);
  }

  public boolean isColumnOrderedDesc(String columnName) {
    int columnIndex = kuduTable.getSchema().getColumnIndex(columnName);
    return descendingOrderedColumnIndexes.contains(columnIndex);
  }

  public boolean isColumnOrderedDesc(int columnIndex) {
    return descendingOrderedColumnIndexes.contains(columnIndex);
  }

  public KuduTable getKuduTable() {
    return kuduTable;
  }

  public AsyncKuduClient getClient() {
    return client;
  }

  /**
   * Implementation of {@link Queryable} based on a {@link CalciteKuduTable}.
   *
   * @param <T> element type
   */
  public static class KuduQueryable<T> extends AbstractTableQueryable<T> {
    public KuduQueryable(final QueryProvider queryProvider, final SchemaPlus schema, final CalciteKuduTable table,
        final String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public Enumerator<T> enumerator() {
      // noinspection unchecked
      final Enumerable<T> enumerable = (Enumerable<T>) getTable().executeQuery(Collections.emptyList(),
          Collections.emptyList(), -1, -1, false, false, new KuduScanStats(), new AtomicBoolean(false), null, null);
      return enumerable.enumerator();
    }

    // Ok this is a bad function, but it makes the above readable and
    // doesn't cause a bunch of messy cast statements
    private CalciteKuduTable getTable() {
      return (CalciteKuduTable) table;
    }

    private CalciteModifiableKuduTable getModifiableTable() {
      return (CalciteModifiableKuduTable) table;
    }

    public Enumerable<Object> query(final List<List<CalciteKuduPredicate>> predicates,
        final List<Integer> fieldsIndices, final long limit, final long offset, final boolean sorted,
        final boolean groupByLimited, final KuduScanStats scanStats, final AtomicBoolean cancelFlag) {
      return query(predicates, fieldsIndices, limit, offset, sorted, groupByLimited, scanStats, cancelFlag, null, null);
    }

    /**
     * This is the method that is called by Code generation to run the query. Code
     * generation happens in {@link KuduToEnumerableConverter}
     */
    public Enumerable<Object> query(final List<List<CalciteKuduPredicate>> predicates,
        final List<Integer> fieldsIndices, final long limit, final long offset, final boolean sorted,
        final boolean groupByLimited, final KuduScanStats scanStats, final AtomicBoolean cancelFlag,
        final Function1<Object, Object> projection, final Predicate1<Object> filterFunction) {
      return getTable().executeQuery(predicates, fieldsIndices, limit, offset, sorted, groupByLimited, scanStats,
          cancelFlag, projection, filterFunction);
    }

    public Enumerable<Object> mutateTuples(final List<Integer> columnIndexes, final List<List<RexLiteral>> tuples) {
      CalciteModifiableKuduTable table = getModifiableTable();
      KuduMetaImpl kuduMetaImpl = ((KuduCalciteConnectionImpl) queryProvider).getMeta();
      return Linq4j.singletonEnumerable(kuduMetaImpl.getMutationState(table).mutateTuples(columnIndexes, tuples));
    }

    public Enumerable<Object> mutateRow(final List<Integer> columnIndexes, final List<Object> values) {
      CalciteModifiableKuduTable table = getModifiableTable();
      KuduMetaImpl kuduMetaImpl = ((KuduCalciteConnectionImpl) queryProvider).getMeta();
      return Linq4j.singletonEnumerable(kuduMetaImpl.getMutationState(table).mutateRow(columnIndexes, values));
    }
  }

  /**
   * Return the Integer indices in the Row Projection that match the primary key
   * columns and in the order they need to match. This lays out how to compare two
   * {@code CalciteRow}s and determine which one is smaller.
   * <p>
   * As an example, imagine we have a table (A, B, C, D, E) with primary columns
   * in order of (A, B) and we have a scanner SELECT D, C, E, B, A the
   * projectedSchema will be D, C, E, B, A and the tableSchema will be A, B, C, D,
   * E *this* function will return List(4, 3) -- the position's of A and B within
   * the projection and in the order they need to be sorted by.
   * <p>
   * The returned index list is used by the sorted {@link KuduEnumerable} to merge
   * the results from multiple scanners.
   */
  public List<Integer> getPrimaryKeyColumnsInProjection(final Schema projectedSchema) {
    return getPrimaryKeyColumnsInProjection(kuduTable.getSchema(), projectedSchema);
  }

  @VisibleForTesting
  public static List<Integer> getPrimaryKeyColumnsInProjection(final Schema schema, final Schema projectedSchema) {
    final List<Integer> primaryKeyColumnsInProjection = new ArrayList<>();
    final List<ColumnSchema> columnSchemas = projectedSchema.getColumns();

    // KuduSortRule checks if the prefix of the primary key columns are being
    // filtered and
    // are set to a constant literal, or if the columns being sorted are a
    // prefix of the primary key columns.
    for (ColumnSchema primaryColumnSchema : schema.getPrimaryKeyColumns()) {
      boolean found = false;
      for (int columnIdx = 0; columnIdx < projectedSchema.getColumnCount(); columnIdx++) {
        if (columnSchemas.get(columnIdx).getName().equals(primaryColumnSchema.getName())) {
          primaryKeyColumnsInProjection.add(columnIdx);
          found = true;
          break;
        }
      }
      // If it isn't found, this means this primary key column is not
      // present in the projection. We keep the existing primary key columns
      // that were present in the projection in our list.
      // The list is the order in which rows will be sorted.
      if (!primaryKeyColumnsInProjection.isEmpty() && !found) {
        // Once we have matched at least one primary key column, all the remaining
        // primary key columns that are present in the projection are being sorted on
        break;
      }
    }
    if (primaryKeyColumnsInProjection.isEmpty()) {
      throw new IllegalStateException("There should be at least one primary key column in the" + " projection");
    }
    return primaryKeyColumnsInProjection;
  }

  public CubeTableInfo.EventTimeAggregationType getEventTimeAggregationType() {
    return eventTimeAggregationType;
  }

  /**
   * Return the integer indices of the descending sorted columns in the row
   * projection.
   */
  public List<Integer> getDescendingColumnsIndicesInProjection(final Schema projectedSchema) {
    final List<Integer> columnsInProjection = new ArrayList<>();
    final List<ColumnSchema> columnSchemas = projectedSchema.getColumns();

    for (Integer fieldIndex : descendingOrderedColumnIndexes) {
      final ColumnSchema columnSchema = kuduTable.getSchema().getColumnByIndex(fieldIndex);
      for (int columnIdx = 0; columnIdx < projectedSchema.getColumnCount(); columnIdx++) {
        if (columnSchemas.get(columnIdx).getName().equals(columnSchema.getName())) {
          columnsInProjection.add(columnIdx);
          break;
        }
      }
    }
    return columnsInProjection;
  }

  public List<CalciteKuduTable> getCubeTables() {
    return cubeTables;
  }

  public int getTimestampColumnIndex() {
    return timestampColumnIndex;
  }

  public List<Integer> getDescendingOrderedColumnIndexes() {
    return descendingOrderedColumnIndexes;
  }

  public TableType getTableType() {
    return tableType;
  }

}
