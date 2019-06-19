package com.twilio.raas.sql;

import java.util.Queue;
import com.stumbleupon.async.Callback;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.ColumnSchema;
import java.nio.ByteBuffer;
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
    final Queue<CalciteScannerMessage<CalciteRow>> rowResults;
    final AtomicBoolean scansShouldStop;
    final List<Integer> primaryKeyColumnsInProjection;
    public ScannerCallback(final AsyncKuduScanner scanner,
        final Queue<CalciteScannerMessage<CalciteRow>> rowResults,
        final AtomicBoolean scansShouldStop,
        final Schema tableSchema,
        final Schema projectedSchema) {

        this.scanner = scanner;
        this.rowResults = rowResults;
        this.scansShouldStop = scansShouldStop;
        this.primaryKeyColumnsInProjection = CalciteRow.findPrimaryKeyColumnsInProjection(projectedSchema, tableSchema);
    }

    @Override
    public Deferred<Void> call(final RowResultIterator nextBatch) {
        boolean earlyExit = false;
        try {
            while (nextBatch != null && nextBatch.hasNext()) {
                final RowResult row = nextBatch.next();
                final CalciteScannerMessage<CalciteRow> wrappedRow = new CalciteScannerMessage<>(
                    new CalciteRow(row, primaryKeyColumnsInProjection));
                if (rowResults.offer(wrappedRow) == false) {
                    // failed to add to the results queue, time to stop doing work.
                    logger.error("failed to insert a new row into pending results. Triggering early exit");
                    earlyExit = true;
                    final boolean failedMessage = rowResults.offer(
                        new CalciteScannerMessage<CalciteRow>(
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
                new CalciteScannerMessage<CalciteRow>(
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
}
