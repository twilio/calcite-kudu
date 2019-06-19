package com.twilio.raas.sql;

import org.apache.calcite.linq4j.Enumerable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collections;
import org.apache.calcite.linq4j.AbstractEnumerable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Queue;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.Schema;
import org.jctools.queues.MpscUnboundedArrayQueue;
import org.jctools.queues.SpscUnboundedArrayQueue;

/**
 * An {@link Enumerable} that *can* returns Kudu records in Ascending order on
 * their primary key. It does so by wrapping a {@link List} of
 * {@link CalciteKuduEnumerable}, querying each one for their next row and
 * comparing those rows. This requires each {@code CalciteKuduEnumerable} scan
 * only one {@link org.apache.kudu.client.Partition} within the Kudu Table.
 * This guarantees the first rows coming out of the
 * {@link org.apache.kudu.client.AsyncKuduScanner} will return rows sorted by
 * primary key.
 *
 * Enumerable will return in unsorted order unless
 * {@link SortableEnumerable#setSorted} is called.
 */
public final class SortableEnumerable extends AbstractEnumerable<Object[]> {
    private static final Logger logger = LoggerFactory.getLogger(SortableEnumerable.class);

    private final List<AsyncKuduScanner> scanners;

    private final AtomicBoolean scansShouldStop;
    private final Schema projectedSchema;
    private final Schema tableSchema;

    public boolean sort = false;

    public long limit = 0L;
    public long offset = 0L;

    public SortableEnumerable(
        List<AsyncKuduScanner> scanners,
        final AtomicBoolean scansShouldStop,
        final Schema projectedSchema,
        final Schema tableSchema) {

        this.scanners = scanners;
        this.scansShouldStop = scansShouldStop;
        this.projectedSchema = projectedSchema;
        this.tableSchema = tableSchema;
    }

    public SortableEnumerable setSorted() {
        this.sort = true;
        return this;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public Enumerator<Object[]> unsortedEnumerator(final int numScanners,
        final Queue<CalciteScannerMessage<CalciteRow>> messages) {
        return new Enumerator<Object[]>() {
            private int finishedScanners = 0;
            private Object[] next = null;
            private boolean finished = false;

            @Override
            public boolean moveNext() {
                if (finished) {
                    return false;
                }
                CalciteScannerMessage<CalciteRow> fetched;
                do {
                    fetched = messages.poll();
                    if (fetched != null) {
                        if (fetched.type == CalciteScannerMessage.MessageType.ERROR) {
                            throw new RuntimeException("A scanner failed, failing whole query", fetched.failure.get());
                        }
                        if (fetched.type == CalciteScannerMessage.MessageType.CLOSE) {
                            if (++finishedScanners >= numScanners) {
                                finished = true;
                                return false;
                            }
                        }
                    }

                } while(fetched == null ||
                    fetched.type != CalciteScannerMessage.MessageType.ROW);
                next = fetched.row.get().rowData;
                return true;
            }

            @Override
            public Object[] current() {
                return next;
            }

            @Override
            public void reset() {
                throw new RuntimeException("Cannot reset an UnsortedEnumerable");
            }

            @Override
            public void close() {
                scansShouldStop.set(true);
            }
        };
    }

    public Enumerator<Object[]> sortedEnumerator(final List<Enumerator<CalciteRow>> subEnumerables) {

        return new Enumerator<Object[]>() {
            private Object[] next = null;
            private List<Boolean> enumerablesWithRows = new ArrayList<>(subEnumerables.size());
            private int totalMoves = 0;

            private void moveToOffset() {
                if (offset > 0) {
                    while(totalMoves < offset && moveNext());
                }
            }

            @Override
            public boolean moveNext() {
                // @TODO: is possible for subEnumerables to be empty?
                if (subEnumerables.isEmpty()) {
                    return false;
                }

                if (enumerablesWithRows.isEmpty()) {
                    for (int idx = 0; idx < subEnumerables.size(); idx++) {
                        enumerablesWithRows.add(subEnumerables.get(idx).moveNext());
                    }
                    moveToOffset();
                    logger.debug("Setup scanners {}", enumerablesWithRows);
                }
                CalciteRow smallest = null;
                int chosenEnumerable = -1;
                for (int idx = 0; idx < subEnumerables.size(); idx++) {
                    if (enumerablesWithRows.get(idx)) {
                        final CalciteRow enumerablesNext = subEnumerables.get(idx).current();
                        if (smallest == null) {
                            logger.trace("smallest isn't set setting to {}", enumerablesNext.rowData);
                            smallest = enumerablesNext;
                            chosenEnumerable = idx;
                        }
                        else if (enumerablesNext.compareTo(smallest) < 0) {
                            logger.trace("{} is smaller then {}",
                                enumerablesNext.rowData, smallest.rowData);
                            smallest = enumerablesNext;
                            chosenEnumerable = idx;
                        }
                        else {
                            logger.trace("{} is larger then {}",
                                enumerablesNext.rowData, smallest.rowData);
                        }
                    }
                    else {
                        logger.trace("{} index doesn't have next", idx);
                    }
                }
                if (smallest == null) {
                    return false;
                }
                next = smallest.rowData;
                // Move the chosen one forward. The others have their smallest
                // already in the front of their queues.
                logger.trace("Chosen idx {} to move next", chosenEnumerable);
                enumerablesWithRows.set(chosenEnumerable,
                    subEnumerables.get(chosenEnumerable).moveNext());
                totalMoves++;
                if (limit > 0) {
                    if (totalMoves - offset >= limit) {
                        scansShouldStop.set(true);
                    }
                }
                return limit <= 0 || totalMoves - offset <= limit;
            }

            @Override
            public Object[] current() {
                return next;
            }

            @Override
            public void reset() {
                subEnumerables
                    .stream()
                    .forEach(e -> e.reset());
            }

            @Override
            public void close() {
                subEnumerables.stream()
                    .forEach(enumerable -> enumerable.close());
            }
        };
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        if (sort) {
            return sortedEnumerator(
                scanners
                .stream()
                .map(scanner -> {
                        // Using Unbounded Queues here which is not awesome.
                        final Queue<CalciteScannerMessage<CalciteRow>> rowResults = new SpscUnboundedArrayQueue<>(3000);

                        // Yuck!!! side effect within a mapper. This is because the
                        // callback and the CalciteKuduEnumerable need to both share
                        // queue.
                        scanner.nextRows()
                            .addBothDeferring(new ScannerCallback(scanner, rowResults, scansShouldStop,
                                tableSchema,
                                    projectedSchema));
                        // Limit is not required here. do not use it.
                        return new CalciteKuduEnumerable(
                            rowResults,
                            scansShouldStop
                        );
                    }
                )
                .map(enumerable -> enumerable.enumerator())
                .collect(Collectors.toList()
                ));
        }
        final Queue<CalciteScannerMessage<CalciteRow>> messages = new MpscUnboundedArrayQueue<>(3000);
        scanners
            .stream()
            .forEach(scanner -> {
                    scanner.nextRows()
                        .addBothDeferring(
                            new ScannerCallback(scanner, messages, scansShouldStop, tableSchema, projectedSchema));
                });
        final int numScanners = scanners.size();

        return unsortedEnumerator(numScanners, messages);
    }
}
