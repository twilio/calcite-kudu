package com.twilio.raas.sql;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import com.stumbleupon.async.Callback;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.RowResult;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kudu.client.Partition;
import java.util.List;
import org.apache.kudu.Schema;

import com.stumbleupon.async.Deferred;
import org.apache.kudu.client.AsyncKuduScanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scanner Callback that produces {@link CalciteScannerMessage} into a
 * {@link Queue}. This will contain rows from Kudu in Scanner order which is
 * different from sorted order. To get sorted order out of this {@link Callback}
 * it needs to be used on a {@link AsyncKuduScanner} over exactly one
 * {@link Partition}
 */
final public class ScannerCallback
    implements Callback<Deferred<Void>, RowResultIterator> {

    private static final Logger logger = LoggerFactory.getLogger(ScannerCallback.class);
    private static final CalciteScannerMessage<CalciteRow> CLOSE_MESSAGE = CalciteScannerMessage.<CalciteRow>createEndMessage();

    final AsyncKuduScanner scanner;
    final BlockingQueue<CalciteScannerMessage<CalciteRow>> rowResults;
    final AtomicBoolean scansShouldStop;
    final AtomicBoolean cancelFlag;
    final AtomicBoolean earlyExit = new AtomicBoolean(false);
    final List<Integer> primaryKeyColumnsInProjection;
    final List<Integer> descendingSortedFieldIndices;
    final KuduScanStats scanStats;

    public ScannerCallback(final CalciteKuduTable calciteKuduTable,
                           final AsyncKuduScanner scanner,
                           final BlockingQueue<CalciteScannerMessage<CalciteRow>> rowResults,
                           final AtomicBoolean scansShouldStop,
                           final AtomicBoolean cancelFlag,
                           final Schema projectedSchema,
                           final KuduScanStats scanStats,
                           final boolean isScannerSorted) {
        this.scanner = scanner;
        this.rowResults = rowResults;
        this.scansShouldStop = scansShouldStop;
        // if the scanner is sorted, KuduEnumerable has to merge the results from multiple
        // scanners (by picking the smallest row order by primary key key columns)
        this.primaryKeyColumnsInProjection = isScannerSorted ?
          calciteKuduTable.getPrimaryKeyColumnsInProjection(projectedSchema) : new ArrayList<>();
        this.descendingSortedFieldIndices =
          calciteKuduTable.getDescendingColumnsIndicesInProjection(projectedSchema);
        this.scanStats = scanStats;
        // @NOTE: this can be NULL. Need to check it.
        // @SEE: org.apache.calcite.plan.AbstractRelOptPlanner(RelOptCostFactory, Context)
        this.cancelFlag = cancelFlag;

        logger.debug("ScannerCallback created for scanner" + scanner);
    }

    /**
     * After an Batch is completed, this method should be called to fetch the next
     */
    public void nextBatch() {
        // @TODO: How to protect this method from being called while a batch is being processed?

        // If the scanner can continue and we are not stopping
        if (scanner.hasMoreRows() && !earlyExit.get() && !scansShouldStop.get() &&
            // allow `null` as cancel flag isn't guaranteed to be set. Instead of handling null
            // in constructor check it here, .get() can be costly as it is atomic.
            (cancelFlag == null || !cancelFlag.get())) {
            scanner.nextRows().addCallbackDeferring(this);
        }
        else {
            // Else -> scanner has completed, notify the consumer of rowResults
            try {
                // This blocks to ensure the query finishes.
                logger.debug("Closing scanner: {} {} {} {}",
                    scanner.hasMoreRows(), earlyExit.get(), scansShouldStop.get(), cancelFlag.get());
                rowResults.put(CLOSE_MESSAGE);
            } catch (InterruptedException threadInterrupted) {
                logger.error("Interrupted while closing. Means queue is full. Closing scanner");
                Thread.currentThread().interrupt();
            }
            scanner.close();
        }
    }

    @Override
    public Deferred<Void> call(final RowResultIterator nextBatch) {
        scanStats.incrementScannerRpcCount(1L);

        if (nextBatch != null) {
          scanStats.incrementRowsScannedCount(nextBatch.getNumRows());
        }
        try {
            if (!earlyExit.get()) {
                while (nextBatch != null && nextBatch.hasNext()) {
                    final RowResult row = nextBatch.next();
                    final CalciteScannerMessage<CalciteRow> wrappedRow = new CalciteScannerMessage<>(
                            new CalciteRow(row, primaryKeyColumnsInProjection, descendingSortedFieldIndices));
                    // Blocks if the queue is full.
                    // @TODO: How to we protect it from locking up here because nothing is consuming
                    // from the queue.
                    rowResults.put(wrappedRow);
                }
            }
        }
        catch (Exception | Error failure) {
            // Something failed, like row.getDecimal() or something of that nature.
            // this means we have to abort this scan.
            logger.error("Failed to parse out row. Setting early exit", failure);
            earlyExit.set(true);
            try {
              rowResults.put(
                  new CalciteScannerMessage<CalciteRow>(
                      new RuntimeException("Failed to parse results.", failure)));
            }
            catch (InterruptedException ignored) {
              logger.error("Interrupted during put, moving to close scanner");
              Thread.currentThread().interrupt();
            }
        }

        try {
            rowResults.put(new CalciteScannerMessage<CalciteRow>(this));
        }
        catch (InterruptedException ignored) {
            // Set the early exit to protect ourselves and close the scanner.
            earlyExit.set(true);
            scanner.close();
            Thread.currentThread().interrupt();
        }
        return null;
    }
}
