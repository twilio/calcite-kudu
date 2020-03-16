package com.twilio.raas.sql;

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
    final List<Integer> primaryKeyColumnsInProjection;
    final List<Integer> descendingSortedFieldIndices;
    final KuduScanStats scanStats;
    
    public ScannerCallback(final AsyncKuduScanner scanner,
                           final BlockingQueue<CalciteScannerMessage<CalciteRow>> rowResults,
                           final AtomicBoolean scansShouldStop,
                           final Schema tableSchema,
                           final Schema projectedSchema,
        final List<Integer> descendingSortedFieldIndices,
        final KuduScanStats scanStats) {
        this.scanner = scanner;
        this.rowResults = rowResults;
        this.scansShouldStop = scansShouldStop;
        this.primaryKeyColumnsInProjection = CalciteRow.findPrimaryKeyColumnsInProjection(projectedSchema, tableSchema);
        this.descendingSortedFieldIndices = CalciteRow.findColumnsIndicesInProjection(projectedSchema, descendingSortedFieldIndices, tableSchema);
        this.scanStats = scanStats;
        logger.debug("ScannerCallback created for scanner" + scanner);
    }

    @Override
    public Deferred<Void> call(final RowResultIterator nextBatch) {
        boolean earlyExit = false;
        scanStats.incrementScannerNextBatchRpcCount(1L);
        if (nextBatch != null) {
          scanStats.incrementRowsReadCount(nextBatch.getNumRows());
        }
        try {
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
        catch (Exception | Error failure) {
            // Something failed, like row.getDecimal() or something of that nature.
            // this means we have to abort this scan.
            logger.error("Failed to parse out row. Setting early exit", failure);
            earlyExit = true;
            try {
              rowResults.put(
                  new CalciteScannerMessage<CalciteRow>(
                      new RuntimeException("Failed to parse results.", failure)));
            }
            catch (InterruptedException ignored) {
              logger.error("Interrupted during put, moving to close scanner");
            }
        }

        // If the scanner can continue and we are not stopping
        if (scanner.hasMoreRows() && !earlyExit && !scansShouldStop.get()) {
            return scanner.nextRows().addCallbackDeferring(this);
        }

        // Else -> scanner has completed, notify the consumer of rowResults
        try {
          // This blocks to ensure the query finishes.
          rowResults.put(CLOSE_MESSAGE);
        }
        catch (InterruptedException threadInterrupted) {
          logger.error("Interrupted while closing. Means queue is full. Closing scanner");
        }
        scanner.close();
        return null;
    }
}
