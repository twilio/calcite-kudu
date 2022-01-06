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

import com.twilio.kudu.sql.rel.KuduProjectRel;
import com.twilio.kudu.sql.rules.KuduPredicatePushDownVisitor;

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.ReplicaSelection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.sql.SqlKind;

import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.AbstractEnumerable2;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.Schema;

// This class resides in this project under the org.apache namespace

/**
 * An {@link Enumerable} that *can* returns Kudu records in Ascending order on
 * their primary key. It does so by wrapping a {@link List} of
 * {@link CalciteKuduEnumerable}, querying each one for their next row and
 * comparing those rows. This requires each {@code CalciteKuduEnumerable} scan
 * only one {@link org.apache.kudu.client.Partition} within the Kudu Table. This
 * guarantees the first rows coming out of the
 * {@link org.apache.kudu.client.AsyncKuduScanner} will return rows sorted by
 * primary key.
 */
public final class KuduEnumerable extends AbstractEnumerable<Object> implements CloneableEnumerable<Object> {
  private static final Logger logger = LoggerFactory.getLogger(KuduEnumerable.class);

  private final AtomicBoolean scansShouldStop;
  private final AtomicBoolean cancelFlag;

  public final boolean sort;
  public final boolean groupBySorted;
  public final Function1<Object, Object> sortedPrefixKeySelector;
  public final long limit;
  public final long offset;
  public final KuduScanStats scanStats;

  private final List<List<CalciteKuduPredicate>> predicates;
  private final List<Integer> columnIndices;
  private final AsyncKuduClient client;
  private final CalciteKuduTable calciteKuduTable;

  private final Function1<Object, Object> projection;
  private final boolean isSingleObject;
  private final Predicate1<Object> filterFunction;

  /**
   * A KuduEnumerable is an {@link Enumerable} for Kudu that can be configured to
   * be sorted.
   * 
   * @param predicates              list of the filters for each disjoint Kudu
   *                                Scan
   * @param columnIndices           the column indexes to fetch from the table
   * @param client                  Kudu client that will execute the scans
   * @param calciteKuduTable        table metadata for the scan
   * @param limit                   the number of rows this should return. -1
   *                                indicates no limit
   * @param offset                  the number of rows from kudu to skip prior to
   *                                returning rows
   * @param sort                    whether or not have Kudu RPCs come back in
   *                                sorted by primary key
   * @param groupBySorted           when sorted, and
   *                                {@link Enumerable#groupBy(Function1, Function0, Function2, Function2)}
   * @param scanStats               a container of scan stats that should be
   *                                updated as the scan executes.
   * @param cancelFlag              boolean indicating the end process has asked
   *                                the query to finish.
   * @param projection              function to translate
   *                                {@link org.apache.kudu.client.RowResult} into
   *                                Calcite
   * @param filterFunction          filter applied to every
   *                                {@link org.apache.kudu.client.RowResult}
   * @param isSingleObject          whether or not Calcite object is an Object or
   *                                an
   * @param sortedPrefixKeySelector the subset of columns being sorted by that are
   *                                a prefix of the primary key of the table, or
   *                                an empty list if the group by and order by
   *                                columns are the same
   */
  public KuduEnumerable(final List<List<CalciteKuduPredicate>> predicates, final List<Integer> columnIndices,
      final AsyncKuduClient client, final CalciteKuduTable calciteKuduTable, final long limit, final long offset,
      final boolean sort, final boolean groupBySorted, final KuduScanStats scanStats, final AtomicBoolean cancelFlag,
      final Function1<Object, Object> projection, final Predicate1<Object> filterFunction, final boolean isSingleObject,
      final Function1<Object, Object> sortedPrefixKeySelector) {
    this.scansShouldStop = new AtomicBoolean(false);
    this.cancelFlag = cancelFlag;
    this.limit = limit;
    this.offset = offset;
    this.projection = projection;
    // if we have an offset always sort by the primary key to ensure the rows are
    // returned
    // in a predictable order
    this.sort = offset > 0 || sort;
    if (groupBySorted && !this.sort) {
      throw new IllegalArgumentException("If groupBySorted is true the results must need to be " + "sorted");
    }
    this.groupBySorted = groupBySorted;
    this.sortedPrefixKeySelector = sortedPrefixKeySelector;
    this.scanStats = scanStats;

    this.predicates = predicates;
    this.columnIndices = columnIndices;
    this.client = client;
    this.calciteKuduTable = calciteKuduTable;
    this.filterFunction = filterFunction;
    this.isSingleObject = isSingleObject;
  }

  @VisibleForTesting
  List<AsyncKuduScanner> getScanners() {
    return createScanners();
  }

  private boolean checkLimitReached(int totalMoves) {
    // handling of limit and/or offset for groupBySorted is done in the groupBy
    // method
    if (limit > 0 && !groupBySorted) {
      long moveOffset = offset > 0 ? offset : 0;
      if (totalMoves - moveOffset > limit) {
        return true;
      }
    }
    return false;
  }

  public Enumerator<Object> unsortedEnumerator(final List<AsyncKuduScanner> scanners,
      final BlockingQueue<CalciteScannerMessage<CalciteRow>> messages) {
    return new Enumerator<Object>() {
      private int finishedScanners = 0;
      private Object next = null;
      private boolean finished = false;
      private int totalMoves = 0;
      private boolean movedToOffset = false;

      private void moveToOffset() {
        movedToOffset = true;
        if (offset > 0) {
          while (totalMoves < offset && moveNext())
            ;
        }
      }

      @Override
      public boolean moveNext() {
        if (finished) {
          return false;
        }
        if (!movedToOffset) {
          moveToOffset();
        }
        CalciteScannerMessage<CalciteRow> fetched;
        do {
          try {
            fetched = messages.poll(350, TimeUnit.MILLISECONDS);
          } catch (InterruptedException interrupted) {
            fetched = CalciteScannerMessage.createEndMessage();
            Thread.currentThread().interrupt();
          }
          if (fetched != null) {
            if (fetched.type == CalciteScannerMessage.MessageType.ERROR) {
              final Optional<Exception> failureReason = fetched.failure;
              if (failureReason.isPresent()) {
                throw new RuntimeException("A scanner failed, failing whole query", failureReason.get());
              } else {
                throw new RuntimeException("A scanner failed, failed for unreported reason. Failing query");
              }
            }
            if (fetched.type == CalciteScannerMessage.MessageType.CLOSE) {
              if (++finishedScanners >= scanners.size()) {
                finished = true;
                return false;
              }
            }
            if (fetched.type == CalciteScannerMessage.MessageType.BATCH_COMPLETED) {
              final Optional<ScannerCallback> callback = fetched.callback;
              if (callback.isPresent()) {
                callback.get().nextBatch();
              } else {
                logger.error(
                    "Scanner sent a BATCH_COMPLETED message but didn't provide a reference to it. This shouldn't happen");
                return false;
              }
            }
          }

        } while (fetched == null || fetched.type != CalciteScannerMessage.MessageType.ROW);
        // Indicates this is the first move.
        if (next == null) {
          scanStats.setTimeToFirstRowMs();
        }
        final Optional<CalciteRow> rowData = fetched.row;
        if (rowData.isPresent()) {
          next = rowData.get().getRowData();
        } else {
          logger.error("Polled a {} message and expected a CalciteRow. The message doesn't contain a row: {} ",
              fetched.type, fetched);
          return false;
        }
        totalMoves++;
        boolean limitReached = checkLimitReached(totalMoves);
        if (limitReached) {
          scansShouldStop.set(true);
        }
        return !limitReached;
      }

      @Override
      public Object current() {
        return next;
      }

      @Override
      public void reset() {
        throw new RuntimeException("Cannot reset an UnsortedEnumerable");
      }

      @Override
      public void close() {
        scansShouldStop.set(true);
        scanStats.setTotalTimeMs();
        List<ScannerMetrics> scannerMetricsList = scanners.stream().map(scanner -> new ScannerMetrics(scanner))
            .collect(Collectors.toList());
        scanStats.addScannerMetricsList(scannerMetricsList);
      }
    };
  }

  public Enumerator<Object> sortedEnumerator(final List<AsyncKuduScanner> scanners,
      final List<Enumerator<CalciteRow>> subEnumerables) {

    return new Enumerator<Object>() {
      private Object next = null;
      private List<Boolean> enumerablesWithRows = new ArrayList<>(subEnumerables.size());
      private int totalMoves = 0;

      private void moveToOffset() {
        // handling of limit and/or offset for groupBySorted is done in the groupBy
        // method
        if (offset > 0 && !groupBySorted) {
          while (totalMoves < offset && moveNext())
            ;
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
              logger.trace("smallest isn't set setting to {}", enumerablesNext.getRowData());
              smallest = enumerablesNext;
              chosenEnumerable = idx;
            } else if (enumerablesNext.compareTo(smallest) < 0) {
              logger.trace("{} is smaller then {}", enumerablesNext.getRowData(), smallest.getRowData());
              smallest = enumerablesNext;
              chosenEnumerable = idx;
            } else {
              logger.trace("{} is larger then {}", enumerablesNext.getRowData(), smallest.getRowData());
            }
          } else {
            logger.trace("{} index doesn't have next", idx);
          }
        }
        if (smallest == null) {
          return false;
        }
        // Indicates this is the first move.
        if (next == null) {
          scanStats.setTimeToFirstRowMs();
        }
        next = smallest.getRowData();

        // Move the chosen one forward. The others have their smallest
        // already in the front of their queues.
        logger.trace("Chosen idx {} to move next", chosenEnumerable);
        enumerablesWithRows.set(chosenEnumerable, subEnumerables.get(chosenEnumerable).moveNext());
        totalMoves++;
        boolean limitReached = checkLimitReached(totalMoves);

        if (limitReached) {
          scansShouldStop.set(true);
        }
        return !limitReached;
      }

      @Override
      public Object current() {
        return next;
      }

      @Override
      public void reset() {
        subEnumerables.stream().forEach(e -> e.reset());
      }

      @Override
      public void close() {
        subEnumerables.stream().forEach(enumerable -> enumerable.close());
        scanStats.setTotalTimeMs();
        List<ScannerMetrics> scannerMetricsList = scanners.stream().map(scanner -> new ScannerMetrics(scanner))
            .collect(Collectors.toList());
        scanStats.addScannerMetricsList(scannerMetricsList);
      }
    };
  }

  public Schema getTableSchema() {
    return this.calciteKuduTable.getKuduTable().getSchema();
  }

  @Override
  public Enumerator<Object> enumerator() {
    final List<AsyncKuduScanner> scanners = createScanners();

    if (scanners.isEmpty()) {
      // if there are predicates but they result in an empty scan list that means this
      // query
      // returns no rows (for eg. querying for dates which don't match any partitions)
      return Linq4j.emptyEnumerator();
    }

    final Schema projectedSchema = scanners.get(0).getProjectionSchema();

    if (sort) {
      final List<ScannerCallback> callbacks = scanners.stream().map(scanner -> {
        final BlockingQueue<CalciteScannerMessage<CalciteRow>> rowResults = new LinkedBlockingQueue<>();
        return new ScannerCallback(calciteKuduTable, scanner, rowResults, scansShouldStop, cancelFlag, projectedSchema,
            scanStats, true, projection, filterFunction, isSingleObject);
      }).collect(Collectors.toList());
      callbacks.stream().forEach(callback -> callback.nextBatch());

      return sortedEnumerator(scanners, callbacks.stream().map(callback -> {
        return new CalciteKuduEnumerable(callback.rowResults, scansShouldStop);
      }).map(enumerable -> enumerable.enumerator()).collect(Collectors.toList()));
    }
    final BlockingQueue<CalciteScannerMessage<CalciteRow>> messages = new LinkedBlockingQueue<>();
    scanners.stream().map(scanner -> {
      return new ScannerCallback(calciteKuduTable, scanner, messages, scansShouldStop, cancelFlag, projectedSchema,
          scanStats, false, projection, filterFunction, isSingleObject);
    }).forEach(callback -> callback.nextBatch());

    return unsortedEnumerator(scanners, messages);
  }

  @Override
  public <TKey, TAccumulate, TResult> Enumerable<TResult> groupBy(Function1<Object, TKey> keySelector,
      Function0<TAccumulate> accumulatorInitializer, Function2<TAccumulate, Object, TAccumulate> accumulatorAdder,
      Function2<TKey, TAccumulate, TResult> resultSelector) {
    // When Grouping rows but the aggregation is not sorted by primary key direction
    // or there is no
    // limit to the grouping, read every single matching row for this query.
    // This implies sorted = false.
    if (!groupBySorted) {
      return EnumerableDefaults.groupBy(getThis(), keySelector, accumulatorInitializer, accumulatorAdder,
          resultSelector);
    }

    int uniqueGroupCount = 0;
    TKey lastKey = null;
    Object lastSortedKey = null;

    // groupFetchLimit calculates it's size based on offset. When offset is present,
    // it needs to
    // skip an equivalent number of unique group keys
    long groupFetchLimit = Long.MAX_VALUE;
    if (offset > 0 && limit > 0) {
      groupFetchLimit = limit + offset;
    } else if (offset > 0) {
      groupFetchLimit = offset;
    } else if (limit > 0) {
      groupFetchLimit = limit;
    }
    final Queue<TResult> sortedResults = new LinkedList<TResult>();

    try (Enumerator<Object> objectEnumeration = getThis().enumerator()) {
      TAccumulate accumulator = null;

      while (objectEnumeration.moveNext()) {
        Object o = objectEnumeration.current();
        final TKey key = keySelector.apply(o);

        if (sortedPrefixKeySelector != null) {
          final Object sortedKey = sortedPrefixKeySelector.apply(o);
          // If sortedPrefixKeySelector is not null, we can only stop reading rows when
          // the
          // sorted key prefix changes and we have enough unique groups since the rows
          // have to be
          // sorted on the client we have to read all the rows that have the same sorted
          // primary key prefix)
          if (lastSortedKey != null && !sortedKey.equals(lastSortedKey) && uniqueGroupCount > groupFetchLimit) {
            logger.debug("sortedKey {} lastSortedKey {} uniqueGroupCount {} groupFetchLimit {}", sortedKey,
                lastSortedKey, uniqueGroupCount, groupFetchLimit);
            break;
          }
          lastSortedKey = sortedKey;
          logger.debug("sortedKey {} uniqueGroupCount{}", sortedKey, uniqueGroupCount);
        }

        // If there hasn't been a key yet or if there is a new key
        if (lastKey == null || !key.equals(lastKey)) {
          // If there is an accumulator, save the results into the queue and reset
          // accumulator.
          if (accumulator != null) {
            sortedResults.offer(resultSelector.apply(lastKey, accumulator));
            accumulator = null;
          }
          uniqueGroupCount++;

          // sortedPrefixKeySelector is null means we don't have to sort results on the
          // client
          // When we have seen limit + 1 unique group by keys, exit.
          // or in the case of an offset, limit + offset + 1 unique group by keys.
          if (sortedPrefixKeySelector == null && uniqueGroupCount > groupFetchLimit) {
            break;
          }
          logger.debug("key {} uniqueGroupCount{}", key, uniqueGroupCount);
          lastKey = key;
        }

        // We can only skip rows if we don't need to sort the returned rows on the
        // client
        // (sortedPrefixKeySelector is null)
        if (sortedPrefixKeySelector == null && offset > 0 && uniqueGroupCount <= offset) {
          // We are still skipping group by keys.
          continue;
        }

        // First Kudu record matching key. Init the accumulator function.
        if (accumulator == null) {
          accumulator = accumulatorInitializer.apply();
        }
        accumulator = accumulatorAdder.apply(accumulator, o);
      }

      // If the source Enumerator -- objectEnumeration -- runs out of rows and we have
      // an accumulator in progress
      // Apply it and save it.
      if (accumulator != null) {
        sortedResults.offer(resultSelector.apply(lastKey, accumulator));
      }
    }
    return new AbstractEnumerable2<TResult>() {
      @Override
      public Iterator<TResult> iterator() {
        return sortedResults.iterator();
      }
    };
  }

  private List<AsyncKuduScanner> createScanners() {
    // This builds a List AsyncKuduScanners.
    // Each member of this list represents an OR query on a given partition
    // in Kudu Table
    List<AsyncKuduScanner> scanners = predicates.stream().map(subScan -> {
      KuduScanToken.KuduScanTokenBuilder tokenBuilder = client.syncClient()
          .newScanTokenBuilder(calciteKuduTable.getKuduTable());
      // Allows for consistent row order in reads as it puts in ORDERED by Pk when
      // faultTolerant is set to true
      // setFaultTolerant to true sets the ReadMode to READ_AT_SNAPSHOT
      // setting snapshot time to 1ms before currentTimestamp to avoid latency issues.
      tokenBuilder.snapshotTimestampMicros(System.currentTimeMillis() * 1000 - 1000);
      tokenBuilder.setFaultTolerant(true);

      if (!columnIndices.isEmpty()) {
        tokenBuilder.setProjectedColumnIndexes(columnIndices);
      }
      // Push down the limit if present AND
      // 1. Not doing a group aggregation.
      // 2. All the predicates are pushed into the scan.
      // When those are true, the Scanners can inform the datanode that it only
      // requires a small number of rows.
      if (limit > 0 && !groupBySorted && filterFunction == Predicate1.TRUE) {
        if (offset > 0) {
          tokenBuilder.limit(offset + limit);
        } else {
          tokenBuilder.limit(limit);
        }
      }
      subScan.stream().forEach(predicate -> {
        tokenBuilder.addPredicate(predicate.toPredicate(calciteKuduTable));
      });
      return tokenBuilder.build();
    }).flatMap(tokens -> tokens.stream().map(token -> {
      try {
        KuduScanner scanner = token.intoScanner(client.syncClient());
        Field asyncScannerField = KuduScanner.class.getDeclaredField("asyncScanner");
        asyncScannerField.setAccessible(true);
        return (AsyncKuduScanner) asyncScannerField.get(scanner);
      } catch (Exception e) {
        throw new RuntimeException("Failed to setup scanner from token.", e);
      }
    })).collect(Collectors.toList());

    if (predicates.isEmpty()) {
      // Scan the whole table !
      final AsyncKuduScanner.AsyncKuduScannerBuilder allBuilder = client
          .newScannerBuilder(calciteKuduTable.getKuduTable());
      if (!columnIndices.isEmpty()) {
        allBuilder.setProjectedColumnIndexes(columnIndices);
      }
      scanners = Collections.singletonList(allBuilder.build());
    }
    return scanners;
  }

  @Override
  public CloneableEnumerable<Object> clone(final List<List<CalciteKuduPredicate>> conjunctions) {
    // The result of the merge can be an empty list. That means we are scanning
    // everything.
    // @TODO: can we generate unique predicates? What happens when it contains the
    // same one.
    final List<List<CalciteKuduPredicate>> merged = KuduPredicatePushDownVisitor.mergePredicateLists(SqlKind.AND,
        this.predicates, conjunctions);
    return new KuduEnumerable(merged, columnIndices, client, calciteKuduTable, limit, offset, sort, groupBySorted,
        scanStats, cancelFlag, projection, filterFunction, isSingleObject, sortedPrefixKeySelector);
  }

  /**
   * Return a function that accepts the Left hand sides rows and creates a new
   * {@code SortableEnumerable} that will match the batch of rows.
   *
   * @param joinNode The {@link Join} relation for this nest join.
   *
   * @return a function that produces another {@code SortableEnumerable} that
   *         matches the batches passed in.
   */
  public Function1<List<Object>, Enumerable<Object>> nestedJoinPredicates(final Join joinNode) {
    final Project rightSideProjection;
    if (joinNode.getRight().getInput(0) instanceof KuduProjectRel) {
      rightSideProjection = (KuduProjectRel) joinNode.getRight().getInput(0);
    } else {
      rightSideProjection = null;
    }
    final List<TranslationPredicate> rowTranslators = joinNode.getCondition()
        .accept(new TranslationPredicate.ConditionTranslationVisitor(joinNode.getLeft().getRowType().getFieldCount(),
            rightSideProjection));
    return new NestedJoinFactory(1000, rowTranslators, this);
  }
}
