package com.twilio.raas.sql;

import org.apache.calcite.linq4j.DefaultEnumerable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Queue;
import org.apache.calcite.linq4j.Enumerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calcite implementation layer that represents a result set of a scan.
 */
public final class CalciteKuduEnumerable extends DefaultEnumerable<Object[]> {
    private static final Logger logger = LoggerFactory.getLogger(CalciteKuduEnumerable.class);

    private int totalMoves = 0;
    private CalciteScannerMessage<Object[]> next = null;

    private final Queue<CalciteScannerMessage<Object[]>> rowResults;
    private final int numScanners;
    private final int limit;
    private final AtomicBoolean shouldStop;

    int closedScansCounter = 0;
    boolean finished = false;

    /**
     * Create Enumerable with a Queue of results, count of all the
     * scans for this, any limit ( can be less then 0 indicating no
     * limit) an shared integer for scans that have finished and a
     * boolean switch indicating the scan should complete.
     *
     * @param rowResults  shared queue to consume from for all the results
     * @param totalScans  count of all the Kudu scans being run
     * @param limit       limit on the number of rows to return. If {@literal <= 0} then no limit
     * @param shouldStop    shared boolean that indicates termination of all scans.
     */
    public CalciteKuduEnumerable(final Queue<CalciteScannerMessage<Object[]>> rowResults,
                                 final int totalScans,
                                 final int limit,
                                 final AtomicBoolean shouldStop) {
        this.rowResults = rowResults;
        this.numScanners = totalScans;
        this.limit = limit;
        this.shouldStop = shouldStop;
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        return new Enumerator<Object[]>() {
            @Override
            public boolean moveNext() {
                if (finished) {
                    logger.info("returning finished");
                    return false;
                }
                CalciteScannerMessage<Object[]> iterationNext;
                do {
                    iterationNext = rowResults.poll();
                    if (iterationNext != null) {
                        switch (iterationNext.type) {
                        case CLOSE:
                            closedScansCounter++;
                            logger.info("Closing scanner {} out of {}",
                                closedScansCounter, numScanners);
                            break;
                        case ERROR:
                            logger.error("Scanner has a failure",
                                iterationNext.failure.get());
                            break;
                        case ROW:
                            logger.trace("Scanner found a row: {}",
                                iterationNext.row.get());
                        }
                    }

                    // This is a tight spin, would love it if
                    // there was a rowResults.poll(TIMEOUT) but
                    // there is not.
                } while (iterationNext == null ||
                    (closedScansCounter < numScanners &&
                        iterationNext.type == CalciteScannerMessage.MessageType.CLOSE));

                if (iterationNext.type == CalciteScannerMessage.MessageType.CLOSE) {
                    logger.info("No more results in queue, exiting");
                    finished = true;
                    return false;
                }
                next = iterationNext;

                if (limit > 0) {
                    totalMoves++;
                    // Over the limit signal to the scanners
                    // to shut themselves down.
                    if (totalMoves >= limit) {
                        logger.info("Informing Scanners to stop after next scan");
                        shouldStop.set(true);
                    }
                }
                return limit <= 0 || totalMoves <= limit;
            }

            @Override
            public Object[] current() {
                switch (next.type) {
                case ROW:
                    return next.row.get();
                case ERROR:
                    throw new RuntimeException(next.failure.get());
                case CLOSE:
                    throw new RuntimeException("Calling current() where next is CLOSE message. This should never happen");
                }
                throw new RuntimeException("Fell out of current(), this should not happen");
            }

            @Override
            public void reset() {
                throw new IllegalStateException("Cannot reset Kudu Enumerable");
            }

            @Override
            public void close() {
                shouldStop.set(true);
            }
        };
    }

    @Override
    public Iterator<Object[]> iterator() {
        final Enumerator<Object[]> resultsEnumerator = enumerator();
        return new Iterator<Object[]>() {
            Boolean hasNext = null;
            @Override
            public boolean hasNext() {
                if (hasNext == null) {
                    hasNext = resultsEnumerator.moveNext();
                }
                return hasNext;
            }

            @Override
            public Object[] next() {
                if (hasNext == null) {
                    hasNext();
                }
                final Object[] record = resultsEnumerator.current();
                hasNext = resultsEnumerator.moveNext();
                return record;
            }
        };
    }

}
