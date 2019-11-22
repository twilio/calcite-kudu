package com.twilio.raas.sql;

import com.twilio.raas.sql.rules.KuduToEnumerableConverter;
import org.apache.calcite.linq4j.Enumerable;

import java.time.Instant;
import java.util.List;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.Schema;
import org.apache.kudu.ColumnSchema;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.kudu.client.AsyncKuduClient;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;

import java.util.stream.Collectors;
import java.util.Collections;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This class resides in this project under the org.apache namespace
import org.apache.kudu.client.KuduScannerUtil;

/**
 * A {@code CalciteKuduTable} is responsible for returning rows of Objects back.
 * It is responsible for calling the RPC layer of Kudu reparsing them into
 * expected types for the RelTable used in the request.
 *
 * It requires a {@link KuduTable} to be opened and ready to be used.
 */
public final class CalciteKuduTable extends AbstractQueryableTable
    implements TranslatableTable {
    private static final Logger logger = LoggerFactory.getLogger(CalciteKuduTable.class);

    public static final Instant EPOCH_DAY_FOR_REVERSE_SORT = Instant.parse("9999-12-31T00:00:00.000000Z");
    public static final Long EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS = EPOCH_DAY_FOR_REVERSE_SORT.toEpochMilli();
    public static final Long EPOCH_FOR_REVERSE_SORT_IN_MICROSECONDS = org.apache.kudu.util.TimestampUtil.timestampToMicros(new java.sql.Timestamp(EPOCH_FOR_REVERSE_SORT_IN_MILLISECONDS));

    private final KuduTable openedTable;
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
        this.openedTable = openedTable;
        this.client = client;
        this.descendingSortedFieldIndices = descendingSortedFieldIndices;
    }

    /**
     * Builds a mapping from Kudu Schema into relational schema names
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        final RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        final Schema kuduSchema = this.openedTable.getSchema();

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
    public RelNode toRel(RelOptTable.ToRelContext context,
                         RelOptTable relOptTable) {

        final RelOptCluster cluster = context.getCluster();
        return new KuduQuery(cluster,
                             cluster.traitSetOf(KuduRel.CONVENTION),
                             relOptTable,
                             this.openedTable,
                             this.descendingSortedFieldIndices,
                             this.getRowType(context.getCluster().getTypeFactory()));
    }

    /**
     * Run the query against the kudu table {@link openedTable}. {@link KuduPredicate} are the
     * filters to apply to the query, {@link kuduFields} are the columns to return in the
     * response and finally {@link limit} is used to limit the results that come back.
     *
     * @param predicates    each member in the first list represents a single scan.
     * @param columnIndices the fields ordinals to select out of Kudu
     * @param limit         process the results until limit is reached. If less then 0,
     *                       no limit is enforced
     * @param offset        skip offset number of rows before returning results
     * @paran sorted        whether to return rows in sorted order
     *
     * @return Enumeration on the objects, Fields conform to {@link CalciteKuduTable#getRowType}.
     */
    public Enumerable<Object[]> executeQuery(final List<List<KuduPredicate>> predicates,
                                             List<Integer> columnIndices, final long limit,
                                             final long offset, final boolean sorted) {
        // Here all the results from all the scans are collected. This is consumed
        // by the Enumerable.enumerator() that this method returns.
        // Set when the enumerator is told to close().
        final AtomicBoolean scansShouldStop = new AtomicBoolean(false);
        // if we have an offset always sort by the primary key to ensure the rows are returned
        // in a predictible order
        final boolean sortByPK = offset>0 || sorted;

        // This  builds a List AsyncKuduScanners.
        // Each member of this list represents an OR query on a given partition
        // in Kudu Table
        List<AsyncKuduScanner> scanners = predicates
            .stream()
            .map(subScan -> {
                    KuduScanToken.KuduScanTokenBuilder tokenBuilder = this.client.syncClient().newScanTokenBuilder(openedTable);
                    if (sorted) {
                        // Allows for consistent row order in reads as it puts in ORDERED by Pk when faultTolerant is set to true
                        tokenBuilder.setFaultTolerant(true);
                    }
                    if (!columnIndices.isEmpty()) {
                        tokenBuilder.setProjectedColumnIndexes(columnIndices);
                    }
                    // we can only push down the limit if  we are ordering by the pk columns
                    // and if there is no offset
                    if (sortByPK && offset==-1 && limit!=-1) {
                        tokenBuilder.limit(limit);
                    }
                    subScan
                        .stream()
                        .forEach(predicate -> {
                                tokenBuilder.addPredicate(predicate);
                            });
                    return tokenBuilder.build();
                })
            .flatMap(tokens -> {
                    return tokens
                        .stream()
                        .map(token -> {
                                try {
                                    return KuduScannerUtil.deserializeIntoAsyncScanner(token.serialize(), client, openedTable);
                                }
                                catch (java.io.IOException ioe) {
                                    throw new RuntimeException("Failed to setup scanner from token.", ioe);
                                }
                            });
                })
            .collect(Collectors.toList());

        if (scanners.isEmpty()) {
            // Scan the whole table !
            final AsyncKuduScanner.AsyncKuduScannerBuilder allBuilder = this.client.newScannerBuilder(this.openedTable);
            if (!columnIndices.isEmpty()) {
                allBuilder.setProjectedColumnIndexes(columnIndices);
            }
            scanners = Collections.singletonList(allBuilder.build());
        }

        // Shared integers between the scanners and the consumer of the rowResults
        // queue. The consumer of the rowResults will attempt to poll from
        // rowResults, and process different message types.
        final int numScanners = scanners.size();
        final Schema projectedSchema;
        if (scanners.size() > 0) {
            projectedSchema = scanners.get(0).getProjectionSchema();
        }
        else {
            projectedSchema = openedTable.getSchema();
        }

        return new SortableEnumerable(scanners, scansShouldStop, projectedSchema,
                openedTable.getSchema(), limit, offset, sorted, descendingSortedFieldIndices);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
                                        SchemaPlus schema, String tableName) {
        return new KuduQueryable<>(queryProvider, schema, this, tableName);
    }

    /** Implementation of {@link Queryable} based on
     * a {@link CalciteKuduTable}.
     *
     * @param <T> element type */
    public static class KuduQueryable<T> extends AbstractTableQueryable<T> {
        public KuduQueryable(QueryProvider queryProvider, SchemaPlus schema,
                             CalciteKuduTable table, String tableName) {
            super(queryProvider, schema, table, tableName);
        }

        public Enumerator<T> enumerator() {
            //noinspection unchecked
            final Enumerable<T> enumerable =
                (Enumerable<T>) getTable().executeQuery(Collections.emptyList(),
                        Collections.emptyList(), -1, -1, false);
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
        public Enumerable<Object[]> query(List<List<CalciteKuduPredicate>> predicates,
                                          List<Integer> fieldsIndices,
                                          long limit, long offset, boolean sorted) {
            return getTable().executeQuery(predicates
                                           .stream()
                                           .map(subList -> {
                                                   return subList
                                                       .stream()
                                                       .map(p ->  p.toPredicate(getTable().openedTable.getSchema(), getTable().descendingSortedFieldIndices))
                                                       .collect(Collectors.toList());
                                               })
                .collect(Collectors.toList()), fieldsIndices, limit, offset, sorted);
        }
    }


}
