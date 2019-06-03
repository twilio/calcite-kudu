package com.twilio.raas.sql;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.DataContext;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import java.util.Map;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.Schema;
import org.apache.kudu.ColumnSchema;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import java.util.concurrent.CountDownLatch;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.calcite.linq4j.AbstractEnumerable2;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kudu.client.AsyncKuduScanner;
import com.stumbleupon.async.Callback;
import org.apache.calcite.linq4j.Enumerator;
import com.stumbleupon.async.Deferred;
import java.util.concurrent.TimeUnit;
import org.apache.kudu.client.KuduPredicate;
import java.util.Iterator;
import org.apache.kudu.client.KuduScanToken;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.kudu.shaded.com.google.protobuf.CodedInputStream;
import org.apache.kudu.client.Client.ScanTokenPB;
import org.apache.kudu.client.AbstractKuduScannerBuilder;
import java.util.ArrayList;
import org.apache.kudu.Common;
import org.apache.kudu.client.ReplicaSelection;
import java.util.Collections;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Queue;
import org.jctools.queues.SpscChunkedArrayQueue;
import org.jctools.queues.MpscChunkedArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.util.Optional;

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
    private static final CalciteScannerMessage<Object[]> CLOSE_MESSAGE = CalciteScannerMessage.<Object[]>createEndMessage();


    private final KuduTable openedTable;
    private final AsyncKuduClient client;

    /**
     * Create the {@code CalciteKuduTable} for a physical scan over
     * the provided {@link KuduTable}. {@code KuduTable} must exist
     * and be opened.
     */
    public CalciteKuduTable(final KuduTable openedTable, final AsyncKuduClient client) {
        super(Object[].class);
        this.openedTable = openedTable;
        this.client = client;
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
        return new KuduQuery(cluster, cluster.traitSetOf(KuduRel.CONVENTION),
                             relOptTable, this.openedTable,
                             this.getRowType(context
                                             .getCluster()
                                             .getTypeFactory()));
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
     *
     * @return Enumeration on the objects, Fields conform to {@link CalciteKuduTable#getRowType}.
     */
    public Enumerable<Object[]> executeQuery(final List<List<KuduPredicate>> predicates, List<Integer> columnIndices, final int limit) {
        // Here all the results from all the scans are collected. This is consumed
        // by the Enumerable.enumerator() that this method returns.
        // Set when the enumerator is told to close().
        final AtomicBoolean scansShouldStop = new AtomicBoolean(false);

        // This  builds a List AsyncKuduScanners.
        // Each member of this list represents an OR query and can be
        // executed in parallel.
        // @TODO: Evaluate using token api to further break down an
        // AsyncKududScanner into indiviual tokens for more
        // parallelization.
        List<AsyncKuduScanner> scanners = predicates
            .stream()
            .map(subScan -> {
                    AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder = this.client.newScannerBuilder(openedTable);
                    if (!columnIndices.isEmpty()) {
                        scannerBuilder.setProjectedColumnIndexes(columnIndices);
                    }
                    subScan
                        .stream()
                        .forEach(predicate -> {
                                scannerBuilder.addPredicate(predicate);
                            });
                    return scannerBuilder.build();
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

        final Queue<CalciteScannerMessage<Object[]>> rowResults;
        if (numScanners == 1) {
            rowResults = new SpscChunkedArrayQueue<CalciteScannerMessage<Object[]>>(3000);
        }
        else {
            rowResults = new MpscChunkedArrayQueue<CalciteScannerMessage<Object[]>>(3000);
        }

        final Enumerable<Object[]> enumeration = new CalciteKuduEnumerable(
            rowResults,
            numScanners,
            limit,
            scansShouldStop);
        for (AsyncKuduScanner scanner: scanners) {
            scanner.nextRows()
                .addBothDeferring(new Callback<Deferred<Void>, RowResultIterator>() {
                        @Override
                        public Deferred<Void> call(final RowResultIterator nextBatch) {
                            boolean earlyExit = false;
                            try {
                                while (nextBatch != null && nextBatch.hasNext()) {
                                    final RowResult row = nextBatch.next();
                                    final Object[] rawRow = new Object[row.getColumnProjection().getColumns().size()];

                                    int columnIndex = 0;
                                    for (ColumnSchema columnType: row.getColumnProjection().getColumns()) {
                                        if (row.isNull(columnIndex)) {
                                            rawRow[columnIndex] = null;
                                        }
                                        else {
                                            switch(columnType.getType()) {
                                            case INT8:
                                                rawRow[columnIndex] = row.getByte(columnIndex);
                                                break;
                                            case INT16:
                                                rawRow[columnIndex] = row.getShort(columnIndex);
                                                break;
                                            case INT32:
                                                rawRow[columnIndex] = row.getInt(columnIndex);
                                                break;
                                            case INT64:
                                                rawRow[columnIndex] = row.getLong(columnIndex);
                                                break;
                                            case STRING:
                                                rawRow[columnIndex] = row.getString(columnIndex);
                                                break;
                                            case BOOL:
                                                rawRow[columnIndex] = row.getBoolean(columnIndex);
                                                break;
                                            case FLOAT:
                                                rawRow[columnIndex] = row.getFloat(columnIndex);
                                                break;
                                            case DOUBLE:
                                                rawRow[columnIndex] = row.getDouble(columnIndex);
                                                break;
                                            case UNIXTIME_MICROS:
                                                // @TODO: is this the right response type?
                                                rawRow[columnIndex] = row.getTimestamp(columnIndex).toInstant().toEpochMilli();
                                                break;
                                            case DECIMAL:
                                                rawRow[columnIndex] = row.getDecimal(columnIndex);
                                                break;
                                            default:
                                                final ByteBuffer byteBuffer = row.getBinary(columnIndex);
                                                byteBuffer.rewind();
                                                byte[] returnedValue = new byte[byteBuffer.remaining()];
                                                byteBuffer.get(returnedValue);
                                                rawRow[columnIndex] = returnedValue;
                                                break;
                                            }
                                        }
                                        columnIndex++;
                                    }
                                    final CalciteScannerMessage<Object[]> wrappedRow = new CalciteScannerMessage<>(
                                        rawRow);
                                    if (rowResults.offer(wrappedRow) == false) {
                                        // failed to add to the results queue, time to stop doing work.
                                        logger.error("failed to insert a new row into pending results. Triggering early exit");
                                        earlyExit = true;
                                        final boolean failedMessage = rowResults.offer(
                                            new CalciteScannerMessage<Object[]>(
                                                new RuntimeException("Queue was full, could not insert row")));
                                        break;
                                    }
                                }
                            }
                            catch (Exception | Error failure) {
                                // Something failed, like row.getDecimal() or something of that nature.
                                // this means we have to abort this scan.
                                logger.error("Failed to parse out row. Setting early exit", failure);
                                earlyExit = true;
                                final boolean failedMessage = rowResults.offer(
                                    new CalciteScannerMessage<Object[]>(
                                        new RuntimeException("Failed to parse results.", failure)));
                            }

                            // If the scanner can continue and we are not stopping
                            if (scanner.hasMoreRows() && !earlyExit && !scansShouldStop.get()) {
                                return scanner.nextRows().addCallbackDeferring(this);
                            }

                            // Else -> scanner has completed, notify the consumer of rowResults
                            final boolean closeResult = rowResults.offer(
                                CLOSE_MESSAGE);
                            if (!closeResult) {
                                logger.error("Failed to put close result message into row results queue");
                            }
                            scanner.close();
                            return null;
                        }
                    });
        }
        return enumeration;
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
                (Enumerable<T>) getTable().executeQuery(Collections.emptyList(), Collections.emptyList(), -1);
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
        public Enumerable<Object[]> query(List<List<CalciteKuduPredicate>> predicates, List<Integer> fieldsIndices) {
            return getTable().executeQuery(predicates
                                           .stream()
                                           .map(subList -> {
                                                   return subList
                                                       .stream()
                                                       .map(p ->  p.toPredicate(getTable().openedTable.getSchema()))
                                                       .collect(Collectors.toList());
                                               })
                                           .collect(Collectors.toList()), fieldsIndices, -1);
        }
    }
}
