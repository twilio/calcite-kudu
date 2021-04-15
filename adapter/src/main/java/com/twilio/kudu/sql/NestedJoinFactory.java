/* Copyright 2021 Twilio, Inc
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.kudu.client.AsyncKuduScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code NestedJoinFactory} is an implementation of {@link Function1} that
 * returns an {@link Enumerable} for the Right hand side of a nested join.
 * <p>
 * This implementation uses a cache to hold objects from the scan to prevent
 * going creating {@link AsyncKuduScanner} for repeated rows
 */
public final class NestedJoinFactory implements Function1<List<Object>, Enumerable<Object>> {

  private static final Logger LOG = LoggerFactory.getLogger(NestedJoinFactory.class);

  private final ResultCache resultCache;
  private final CloneableEnumerable<Object> rootEnumerable;
  private final List<TranslationPredicate> rowTranslators;

  /**
   * Create a Factory that produces {@link Enumerable} that can cache results from
   * previous calls
   *
   * @param capacity       size of the RPC cache
   * @param rowTranslators bindable predicates that depend on the row from the
   *                       left side of the join
   * @param rootEnumerable base enumerable that will be
   *                       {@link CloneableEnumerable#clone(List)}
   */
  public NestedJoinFactory(final int capacity, final List<TranslationPredicate> rowTranslators,
      final CloneableEnumerable<Object> rootEnumerable) {
    this.resultCache = new ResultCache(capacity);
    this.rowTranslators = rowTranslators;
    this.rootEnumerable = rootEnumerable;
  }

  @Override
  public Enumerable<Object> apply(final List<Object> batchFromLeftTable) {
    if (batchFromLeftTable.size() >= resultCache.capacity) {
      LOG.warn(
          "Batch (count: {}) is larger than the result cache size (size: {}). This makes prevents the cache from being effective",
          batchFromLeftTable.size(), resultCache.capacity);
    }

    final List<Enumerator<Object>> enumerators = batchFromLeftTable.stream()
        .map(rowFromLeft -> rowTranslators.stream().map(t -> t.toPredicate((Object[]) rowFromLeft))
            .collect(Collectors.toSet()))
        // Make all the Sub scans unique to reduce work and to ensure this works
        // properly.
        .distinct().map(predicates -> resultCache.compute(predicates, (existingPredicates, existingEnumerator) -> {
          if (existingEnumerator == null) {
            return new CachingEnumerator(
                rootEnumerable.clone(Collections.singletonList(new ArrayList<>(existingPredicates))).enumerator());
          } else {
            existingEnumerator.reset();
            return existingEnumerator;
          }
        })).collect(Collectors.toList());

    return new AbstractEnumerable<Object>() {
      @Override
      public Enumerator<Object> enumerator() {
        return new EnumeratorOfEnumerators(enumerators);
      }
    };
  }

  private final class EnumeratorOfEnumerators implements Enumerator<Object> {
    final List<Enumerator<Object>> enumerators;
    private int currentEnumerator = 0;

    EnumeratorOfEnumerators(final List<Enumerator<Object>> enumerators) {
      this.enumerators = enumerators;
    }

    @Override
    public Object current() {
      return enumerators.get(currentEnumerator).current();
    }

    @Override
    public boolean moveNext() {
      final boolean currentSuccess = enumerators.get(currentEnumerator).moveNext();
      if (currentSuccess) {
        return true;
      }
      while (++currentEnumerator < enumerators.size()) {
        final boolean nextSuccess = enumerators.get(currentEnumerator).moveNext();
        if (nextSuccess) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void reset() {
      enumerators.forEach(e -> e.reset());
      currentEnumerator = 0;
    }

    @Override
    public void close() {
      enumerators.forEach(e -> e.close());
    }
  }

  private final class CachingEnumerator implements Enumerator<Object> {
    private final Enumerator<Object> actualEnumerator;
    private final List<Object> cachedValues = new ArrayList<>();
    private boolean doneScan = false;
    private boolean started = false;
    private int cursor = -1;
    private Object current = null;

    CachingEnumerator(final Enumerator<Object> actualEnumerator) {
      this.actualEnumerator = actualEnumerator;
    }

    @Override
    public Object current() {
      return current;
    }

    @Override
    public boolean moveNext() {
      started = true;
      if (doneScan) {
        final boolean moved = ++cursor < cachedValues.size();
        if (moved) {
          current = cachedValues.get(cursor);
          return true;
        }
        return false;
      } else {
        final boolean scanResult = actualEnumerator.moveNext();
        if (!scanResult) {
          doneScan = true;
          actualEnumerator.close();
        } else {
          current = actualEnumerator.current();
          cachedValues.add(current);
        }
        return scanResult;
      }
    }

    @Override
    public void reset() {
      if (started && !doneScan) {
        actualEnumerator.reset();
      }
      cursor = -1;
    }

    @Override
    public void close() {
      actualEnumerator.close();
      cursor = -1;
    }
  }

  private final class ResultCache extends LinkedHashMap<Set<CalciteKuduPredicate>, CachingEnumerator> {
    /**
     * Default Serialization id for the {@link LinkedHashMap} interface
     */
    private static final long serialVersionUID = 1L;
    private int capacity;

    ResultCache(final int capacity) {
      // Use default capacity and load factor and *finally* set access order to create
      // a proper
      // cache
      super(16, 0.75f, true);
      this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<Set<CalciteKuduPredicate>, CachingEnumerator> eldest) {
      return size() > capacity;
    }
  }
}
