package com.twilio.raas.sql;

import org.apache.calcite.linq4j.DefaultEnumerable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Queue;
import org.apache.calcite.linq4j.Enumerator;

/**
 * Calcite implementation layer that represents a result set of a scan.
 */
public final class CalciteKuduEnumerable extends DefaultEnumerable<Object[]> {
    private int totalMoves = 0;
    private Object[] next = null;
    private AtomicInteger finishedScans;

    private final Queue<Object[]> rowResults;
    private final int numScanners;
    private final int limit;
    private final AtomicBoolean shouldStop;

    /**
     * Create Enumerable with a Queue of results, count of all the
     * scans for this, any limit ( can be less then 0 indicating no
     * limit) an shared integer for scans that have finished and a
     * boolean switch indicating the scan should complete.
     *
     * @param rowResults  shared queue to consume from for all the results
     * @param totalScans  count of all the Kudu scans being run
     * @param limit       limit on the number of rows to return. If {@literal <= 0} then no limit
     * @param finishedScans  shared integer that is incremented when a scan finishes
     * @param shouldStop    shared boolean that indicates termination of all scans.
     */
    public CalciteKuduEnumerable(final Queue<Object[]> rowResults,
                                 final int totalScans,
                                 final int limit,
                                 final AtomicInteger finishedScans,
                                 final AtomicBoolean shouldStop) {
        this.rowResults = rowResults;
        this.numScanners = totalScans;
        this.limit = limit;
        this.finishedScans = finishedScans;
        this.shouldStop = shouldStop;
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        return new Enumerator<Object[]>() {
            @Override
            public boolean moveNext() {

                do {
                    next = rowResults.poll();
                    // This is a tight spin, would love it if
                    // there was a rowResults.poll(TIMEOUT) but
                    // there is not.
                } while (next == null && finishedScans.get() < numScanners);

                if (next == null) {
                    return false;
                }

                if (limit > 0) {
                    totalMoves++;
                    // Over the limit signal to the scanners
                    // to shut themselves down.
                    if (totalMoves >= limit) {
                        shouldStop.set(true);
                    }
                }
                return limit <= 0 || totalMoves < limit;
            }

            @Override
            public Object[] current() {
                return next;
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
                return resultsEnumerator.current() != null;
            }

            @Override
            public Object[] next() {
                if (hasNext == null) {
                    hasNext();
                }
                final Object[] record = resultsEnumerator.current();
                resultsEnumerator.moveNext();
                return record;
            }
        };
    }

}
