package com.twilio.raas.sql;

import com.google.common.collect.ImmutableList;
import com.twilio.raas.sql.rules.KuduToEnumerableConverter;
import org.apache.calcite.linq4j.Enumerable;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
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
import java.util.stream.Collectors;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This class resides in this project under the org.apache namespace


/**
 * A {@code CalciteKuduTable} is responsible for returning rows of Objects back.
 * It is responsible for calling the RPC layer of Kudu reparsing them into
 * expected types for the RelTable used in the request.
 *
 * It requires a {@link KuduTable} to be opened and ready to be used.
 */
public final class CalciteKuduTable extends AbstractQueryableTable
    implements TranslatableTable, ModifiableTable {
    private static final Logger logger = LoggerFactory.getLogger(CalciteKuduTable.class);

    public static final Instant EPOCH_DAY_FOR_REVERSE_SORT = Instant.parse("9999-12-31T00:00:00.000000Z");
    public static final Long EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS = EPOCH_DAY_FOR_REVERSE_SORT.toEpochMilli();
    public static final Long EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS = TimestampUtil.timestampToMicros(
        new Timestamp(EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS));

    private final static Double DIMENSION_TABLE_ROW_COUNT = 1000.0;
    private final static Double CUBE_TABLE_ROW_COUNT = 2000000.0;
    private final static Double FACT_TABLE_ROW_COUNT = 20000000.0;

    private final KuduTable kuduTable;
    private final AsyncKuduClient client;
    private final List<Integer> descendingSortedFieldIndices;

    public CalciteKuduTable(final KuduTable openedTable, final AsyncKuduClient client) {
        this(openedTable, client, Collections.<Integer>emptyList());
    }

    /**
     * Create the {@code CalciteKuduTable} for a physical scan over
     * the provided {@link KuduTable}. {@code KuduTable} must exist
     * and be opened.
     */
    public CalciteKuduTable(final KuduTable openedTable, final AsyncKuduClient client, final List<Integer> descendingSortedFieldIndices) {
        super(Object[].class);
        this.kuduTable = openedTable;
        this.client = client;
        this.descendingSortedFieldIndices = descendingSortedFieldIndices;
    }

    @Override
    public Statistic getStatistic() {
      final String tableName = this.kuduTable.getName();
      // Simple rules.
      // "-" in table name, you are a cube
      // "." in table name, you are a raw fact table
      // "anything else" you are a dimension table.
      final List<ImmutableBitSet> primaryKeys = Collections.singletonList(ImmutableBitSet
          .range(this.kuduTable.getSchema().getPrimaryKeyColumnCount()));
      if (tableName.contains("-")) {
        return Statistics.of(CUBE_TABLE_ROW_COUNT,
            primaryKeys,
            Collections.emptyList(),
            // We don't always sort for two reasons:
            // 1. When applying a Filter we want to also sort that doesn't magically happen by
            //    setting this as a RelCollation
            // 2. Awhile ago we saw performance degrade with always sorting.
            Collections.emptyList());
      }
      if (tableName.contains(".")) {
        return Statistics.of(FACT_TABLE_ROW_COUNT,
                primaryKeys,
                Collections.emptyList(),
                // We don't always sort for two reasons:
                // 1. When applying a Filter we want to also sort that doesn't magically happen by
                //    setting this as a RelCollation
                // 2. Awhile ago we saw performance degrade with always sorting.
                Collections.emptyList());
      }
      return Statistics.of(DIMENSION_TABLE_ROW_COUNT,
                primaryKeys,
                Collections.emptyList(),
                // We don't always sort for two reasons:
                // 1. When applying a Filter we want to also sort that doesn't magically happen by
                //    setting this as a RelCollation
                // 2. Awhile ago we saw performance degrade with always sorting.
                Collections.emptyList());
    }

    /**
     * Builds a mapping from Kudu Schema into relational schema names
     */
    @Override
    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
        final RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        final Schema kuduSchema = this.kuduTable.getSchema();

        for (int i = 0; i < kuduSchema.getColumnCount(); i++) {
            final ColumnSchema currentColumn = kuduSchema.getColumnByIndex(i);
            switch (currentColumn.getType()) {
            case INT8:
                builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.TINYINT)
                    .nullable(currentColumn.isNullable());
                break;
            case INT16:
                builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.SMALLINT)
                    .nullable(currentColumn.isNullable());
                break;
            case INT32:
                builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.INTEGER)
                    .nullable(currentColumn.isNullable());
                break;
            case INT64:
                builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.BIGINT)
                    .nullable(currentColumn.isNullable());
                break;
            case BINARY:
                builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.VARBINARY)
                    .nullable(currentColumn.isNullable());
                break;
            case STRING:
                builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.VARCHAR)
                    .nullable(currentColumn.isNullable());
                break;
            case BOOL:
                builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.BOOLEAN)
                    .nullable(currentColumn.isNullable());
                break;
            case FLOAT:
                builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.FLOAT)
                    .nullable(currentColumn.isNullable());
                break;
            case DOUBLE:
                builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.DOUBLE)
                    .nullable(currentColumn.isNullable());
                break;
            case UNIXTIME_MICROS:
                builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.TIMESTAMP)
                    .nullable(currentColumn.isNullable());
                break;
            case DECIMAL:
                builder.add(currentColumn.getName().toUpperCase(), SqlTypeName.DECIMAL)
                    .nullable(currentColumn.isNullable());
                break;
            }
        }

        return builder.build();
    }

    @Override
    public RelNode toRel(final RelOptTable.ToRelContext context,
                         final RelOptTable relOptTable) {

        final RelOptCluster cluster = context.getCluster();
        return new KuduQuery(cluster,
                             cluster.traitSetOf(KuduRelNode.CONVENTION),
                             relOptTable,
                             this.kuduTable,
                             this.descendingSortedFieldIndices,
                             this.getRowType(context.getCluster().getTypeFactory()));
    }

    /**
     * Run the query against the kudu table {@link kuduTable}. {@link KuduPredicate} are the
     * filters to apply to the query, {@link kuduFields} are the columns to return in the
     * response and finally {@link limit} is used to limit the results that come back.
     *
     * @param predicates     each member in the first list represents a single scan.
     * @param columnIndices  the fields ordinals to select out of Kudu
     * @param limit          process the results until limit is reached. If less then 0,
     *                        no limit is enforced
     * @param offset         skip offset number of rows before returning results
     * @param sorted         whether to return rows in sorted order
     * @param groupByLimited indicates if the groupBy method should be counting unique keys
     *
     * @return Enumeration on the objects, Fields conform to {@link CalciteKuduTable#getRowType}.
     */
    public KuduEnumerable executeQuery(final List<List<CalciteKuduPredicate>> predicates,
                                             final List<Integer> columnIndices, final long limit,
        final long offset, final boolean sorted, final boolean groupByLimited, final KuduScanStats scanStats, final AtomicBoolean cancelFlag) {


        return new KuduEnumerable(
            predicates, columnIndices, this.client, this.kuduTable,
            limit, offset, sorted, descendingSortedFieldIndices,
            groupByLimited, scanStats, cancelFlag);
    }

    /**
     * Called while using {@link java.sql.Statement}
     */
    public int mutateTuples(final List<String> columnNames,
                               final List<List<RexLiteral>> tuples) {
      return new KuduMutation(this, columnNames).mutateTuples(tuples);
    }

    /**
     * Called while using {@link java.sql.PreparedStatement}
     */
    public int mutateRow(final List<String> columnNames, final List<Object> values) {
      return new KuduMutation(this, columnNames).mutateRow(values);
    }

    @Override
    public <T> Queryable<T> asQueryable(final QueryProvider queryProvider,
                                        final SchemaPlus schema, final String tableName) {
        return new KuduQueryable<>(queryProvider, schema, this, tableName);
    }

    @Override
    public Collection getModifiableCollection() {
      return null;
    }

    @Override
    public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table,
                                         Prepare.CatalogReader catalogReader, RelNode child,
                                         TableModify.Operation operation,
                                         List<String> updateColumnList,
                                         List<RexNode> sourceExpressionList, boolean flattened) {
      if (!operation.equals(TableModify.Operation.INSERT)) {
        throw new UnsupportedOperationException("Only INSERT statement is supported");
      }
      return new KuduWrite(
        this.kuduTable,
        cluster,
        table,
        catalogReader,
        child,
        operation,
        updateColumnList,
        sourceExpressionList,
        flattened);
    }

    public boolean isColumnSortedDesc(String columnName) {
      int columnIndex = kuduTable.getSchema().getColumnIndex(columnName);
      return descendingSortedFieldIndices.contains(columnIndex);
    }

    public KuduTable getKuduTable() {
      return kuduTable;
    }

    public AsyncKuduClient getClient() {
      return client;
    }

  /** Implementation of {@link Queryable} based on
     * a {@link CalciteKuduTable}.
     *
     * @param <T> element type */
    public static class KuduQueryable<T> extends AbstractTableQueryable<T> {
        public KuduQueryable(final QueryProvider queryProvider, final SchemaPlus schema,
                             final CalciteKuduTable table, final String tableName) {
            super(queryProvider, schema, table, tableName);
        }

        public Enumerator<T> enumerator() {
            //noinspection unchecked
            final Enumerable<T> enumerable =
                (Enumerable<T>) getTable().executeQuery(Collections.emptyList(),
                    Collections.emptyList(), -1, -1, false, false, new KuduScanStats(), new AtomicBoolean(false));
            return enumerable.enumerator();
        }

        // Ok this is a bad function, but it makes the above readable and
        // doesn't cause a bunch of messy cast statements
        private CalciteKuduTable getTable() {
            return (CalciteKuduTable) table;
        }

        /**
         * This is the method that is called by Code generation to run the query.
         * Code generation happens in {@link KuduToEnumerableConverter}
         */
        public Enumerable<Object> query(final List<List<CalciteKuduPredicate>> predicates,
                                        final List<Integer> fieldsIndices, final long limit,
                                        final long offset, final boolean sorted,
                                        final boolean groupByLimited,
                                        final KuduScanStats scanStats,
                                        final AtomicBoolean cancelFlag) {
            return getTable()
                .executeQuery(
                predicates,
                fieldsIndices,
                limit,
                offset,
                sorted,
                groupByLimited,
                scanStats,
                cancelFlag);
        }

        public Enumerable<Object> mutateTuples(final List<String> columnNames,
                          final List<List<RexLiteral>> tuples) {
          return Linq4j.singletonEnumerable(getTable().mutateTuples(columnNames, tuples));
        }

    public Enumerable<Object> mutateRow(final List<String> columnNames,
                                     final List<Object> values) {
      return Linq4j.singletonEnumerable(getTable().mutateRow(columnNames, values));
    }
  }


}
