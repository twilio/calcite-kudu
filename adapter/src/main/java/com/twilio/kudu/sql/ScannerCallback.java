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

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import com.stumbleupon.async.Callback;
import org.apache.kudu.client.RowResultIterator;
import org.apache.kudu.client.RowResult;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kudu.client.Partition;
import java.util.List;

import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.kudu.Schema;

import com.stumbleupon.async.Deferred;
import org.apache.kudu.client.AsyncKuduScanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scanner Callback that produces {@link CalciteScannerMessage} into a
 * {@link BlockingQueue}. This will contain rows from Kudu in Scanner order
 * which is different from sorted order. To get sorted order out of this
 * {@link Callback} it needs to be used on a {@link AsyncKuduScanner} over
 * exactly one {@link Partition}
 */
final public class ScannerCallback implements Callback<Deferred<Void>, RowResultIterator> {

  private static final Logger logger = LoggerFactory.getLogger(ScannerCallback.class);
  private static final CalciteScannerMessage<CalciteRow> CLOSE_MESSAGE = CalciteScannerMessage
      .<CalciteRow>createEndMessage();

  final AsyncKuduScanner scanner;
  final BlockingQueue<CalciteScannerMessage<CalciteRow>> rowResults;
  final AtomicBoolean scansShouldStop;
  final AtomicBoolean cancelFlag;
  final AtomicBoolean earlyExit = new AtomicBoolean(false);
  final List<Integer> primaryKeyColumnsInProjection;
  final List<Integer> descendingSortedFieldIndices;
  final KuduScanStats scanStats;
  final Function1<Object, Object> projectionMapper;
  final Predicate1<Object> filterFunction;
  final boolean isSingleObject;

  public ScannerCallback(final CalciteKuduTable calciteKuduTable, final AsyncKuduScanner scanner,
      final BlockingQueue<CalciteScannerMessage<CalciteRow>> rowResults, final AtomicBoolean scansShouldStop,
      final AtomicBoolean cancelFlag, final Schema projectedSchema, final KuduScanStats scanStats,
      final boolean isScannerSorted, final Function1<Object, Object> projectionMapper,
      final Predicate1<Object> filterFunction, final boolean isSingleObject) {

    this.scanner = scanner;
    this.rowResults = rowResults;
    this.scansShouldStop = scansShouldStop;
    // if the scanner is sorted, KuduEnumerable has to merge the results from
    // multiple
    // scanners (by picking the smallest row order by primary key key columns)
    this.primaryKeyColumnsInProjection = isScannerSorted
        ? calciteKuduTable.getPrimaryKeyColumnsInProjection(projectedSchema)
        : Collections.emptyList();
    this.descendingSortedFieldIndices = calciteKuduTable.getDescendingColumnsIndicesInProjection(projectedSchema);
    this.scanStats = scanStats;
    // @NOTE: this can be NULL. Need to check it.
    // @SEE: org.apache.calcite.plan.AbstractRelOptPlanner(RelOptCostFactory,
    // Context)
    this.cancelFlag = cancelFlag;

    this.projectionMapper = projectionMapper;
    this.filterFunction = filterFunction;
    this.isSingleObject = isSingleObject;

    logger.debug("ScannerCallback created for scanner" + scanner);
  }

  /**
   * After an Batch is completed, this method should be called to fetch the next
   */
  public void nextBatch() {
    // @TODO: How to protect this method from being called while a batch is being
    // processed?

    // If the scanner can continue and we are not stopping
    if (scanner.hasMoreRows() && !scansShouldStop.get() && !earlyExit.get() &&
    // allow `null` as cancel flag isn't guaranteed to be set. Instead of handling
    // null
    // in constructor check it here, .get() can be costly as it is atomic.
        (cancelFlag == null || !cancelFlag.get())) {
      final Deferred<RowResultIterator> nextRowsRpc = scanner.nextRows();
      nextRowsRpc.addCallbackDeferring(this).addErrback(new Callback<Void, Exception>() {
        @Override
        public Void call(Exception failure) {
          logger.error("Closing scanner with failure and setting earlyExit", failure);
          exitScansWithFailure(failure);
          return null;
        }
      });
    } else {
      // Else -> scanner has completed, notify the consumer of rowResults
      try {
        // This blocks to ensure the query finishes.
        logger.debug("Closing scanner: {} {} {} {}", scanner.hasMoreRows(), earlyExit, scansShouldStop,
            cancelFlag.get());
        rowResults.put(CLOSE_MESSAGE);
      } catch (InterruptedException threadInterrupted) {
        logger.error("Interrupted while closing. Means queue is full. Closing scanner");
        Thread.currentThread().interrupt();
      }
      scanner.close();
    }
  }

  private void exitScansWithFailure(final Exception failure) {
    earlyExit.set(true);
    try {
      rowResults.put(new CalciteScannerMessage<CalciteRow>(failure));
    } catch (InterruptedException ignored) {
      logger.error("Interrupted during put, moving to close scanner");
      Thread.currentThread().interrupt();
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
          if (!filterFunction.apply(row)) {
            continue;
          }
          final CalciteScannerMessage<CalciteRow> wrappedRow;
          if (!isSingleObject) {
            wrappedRow = new CalciteScannerMessage<>(new CalciteRow(row.getSchema(),
                ((Object[]) projectionMapper.apply(row)), primaryKeyColumnsInProjection, descendingSortedFieldIndices));
          } else {
            final Object mapResult = ((Object) projectionMapper.apply(row));
            final Object[] rowData = new Object[] { mapResult };
            wrappedRow = new CalciteScannerMessage<>(
                new CalciteRow(row.getSchema(), rowData, primaryKeyColumnsInProjection, descendingSortedFieldIndices));
          }
          // Blocks if the queue is full.
          // @TODO: How to we protect it from locking up here because nothing is consuming
          // from the queue.
          rowResults.put(wrappedRow);
        }
      }
    } catch (Exception failure) {
      // Something failed, like row.getDecimal() or something of that nature.
      // this means we have to abort this scan.
      logger.error("Failed to parse out row. Setting early exit", failure);
      exitScansWithFailure(failure);
    }

    try {
      rowResults.put(new CalciteScannerMessage<CalciteRow>(this));
    } catch (InterruptedException ignored) {
      // Set the early exit to protect ourselves and close the scanner.
      exitScansWithFailure(ignored);
      Thread.currentThread().interrupt();
    }
    return null;
  }
}
