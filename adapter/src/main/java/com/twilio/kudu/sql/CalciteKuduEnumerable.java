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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Optional;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.AbstractEnumerable;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calcite implementation layer that represents a result set of a scan.
 */
public final class CalciteKuduEnumerable extends AbstractEnumerable<CalciteRow> {
  private static final Logger logger = LoggerFactory.getLogger(CalciteKuduEnumerable.class);

  private CalciteScannerMessage<CalciteRow> next = null;

  private final BlockingQueue<CalciteScannerMessage<CalciteRow>> rowResults;
  private final AtomicBoolean shouldStop;

  int closedScansCounter = 0;
  boolean finished = false;

  /**
   * Create Enumerable with a Queue of results, a shared integer for scans that
   * have finished and a boolean switch indicating the scan should complete.
   *
   * @param rowResults shared queue to consume from for all the results
   * @param shouldStop shared boolean that indicates termination of all scans.
   */
  public CalciteKuduEnumerable(final BlockingQueue<CalciteScannerMessage<CalciteRow>> rowResults,
      final AtomicBoolean shouldStop) {
    this.rowResults = rowResults;
    this.shouldStop = shouldStop;
  }

  @Override
  public Enumerator<CalciteRow> enumerator() {
    return new Enumerator<CalciteRow>() {
      @Override
      public boolean moveNext() {
        if (finished) {
          logger.debug("returning finished");
          return false;
        }
        CalciteScannerMessage<CalciteRow> iterationNext;
        do {
          try {
            iterationNext = rowResults.poll(350, TimeUnit.MILLISECONDS);
          } catch (InterruptedException interrupted) {
            logger.info("Interrupted during poll, closing scanner");
            iterationNext = CalciteScannerMessage.createEndMessage();
            Thread.currentThread().interrupt();
          }
          if (iterationNext != null) {
            switch (iterationNext.type) {
            case CLOSE:
              logger.debug("Closing scanner");
              break;
            case ERROR:
              final Optional<Exception> failure = iterationNext.failure;
              if (failure.isPresent()) {
                logger.error("Scanner has a failure", failure.get());
              } else {
                logger.error("Scanner had an unreported failure");
              }
              break;
            case ROW:
              final Optional<CalciteRow> maybeRow = iterationNext.row;
              if (maybeRow.isPresent()) {
                logger.trace("Scanner found a row: {}", maybeRow.get());
              } else {
                logger.error("ROW message was received but didn't contain row data. This shouldn't happen. Closing");
                iterationNext = CalciteScannerMessage.createEndMessage();
              }
              break;
            case BATCH_COMPLETED:
              final Optional<ScannerCallback> maybeScannerCallback = iterationNext.callback;
              if (maybeScannerCallback.isPresent()) {
                logger.debug("Batch completed for a scanner. Getting next batch");
                maybeScannerCallback.get().nextBatch();
              } else {
                logger.error(
                    "Batch completed message for scanner but no reference to the callback. This shouldn't happen");
                iterationNext = CalciteScannerMessage.createEndMessage();
              }
            }
          }

        } while (iterationNext == null
            || (!iterationNext.isTerminal() && iterationNext.type != CalciteScannerMessage.MessageType.ROW));

        if (iterationNext.isTerminal()) {
          logger.debug("No more results in queue, exiting");
          finished = true;
          return false;
        }
        next = iterationNext;

        return true;
      }

      @Override
      public CalciteRow current() {
        switch (next.type) {
        case ROW:
          final Optional<CalciteRow> maybeRow = next.row;
          if (maybeRow.isPresent()) {
            return maybeRow.get();
          } else {
            throw new RuntimeException("Expected next to have a row, it does not. This shouldn't happen");
          }
        case ERROR:
          final Exception failure = next.failure.orElseGet(() -> new RuntimeException("Unreported failure occurred"));
          throw new RuntimeException("Calling current() on Failed rpc fetch", failure);
        case CLOSE:
          throw new RuntimeException("Calling current() where next is CLOSE message. This should never happen");
        case BATCH_COMPLETED:
          throw new RuntimeException(
              "Calling current() after receiving a BATCH_COMPLETED message. This should never happen");
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
}
