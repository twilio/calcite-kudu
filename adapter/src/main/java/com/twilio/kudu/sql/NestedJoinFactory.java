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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.kudu.client.AsyncKuduScanner;

/**
 * {@code NestedJoinFactory} is an implementation of {@link Function1} that
 * returns an {@link Enumerable} for the Right hand side of a nested join.
 * <p>
 * This implementation uses a cache to hold objects from the scan to prevent
 * going creating {@link AsyncKuduScanner} for repeated rows
 */
public final class NestedJoinFactory implements Function1<List<Object>, Enumerable<Object>> {

  private final KuduRowCache rowCache;

  private final Predicate2<Object, Object> joinCondition;

  private final List<TranslationPredicate> rowTranslators;

  private final KuduEnumerable rootEnumerable;

  public NestedJoinFactory(final Predicate2<Object, Object> joinCondition, final EqualityComparer<Object> compareUtil,
      final List<TranslationPredicate> rowTranslators, final KuduEnumerable rootEnumerable) {
    this.joinCondition = joinCondition;
    this.rowCache = new KuduRowCache(compareUtil, 1000);
    this.rowTranslators = rowTranslators;
    this.rootEnumerable = rootEnumerable;
  }

  @Override
  public Enumerable<Object> apply(final List<Object> batchFromLeftTable) {
    final KuduRowCache.SearchResult cacheResult = rowCache.cacheSearch(batchFromLeftTable, joinCondition);

    // Indicates it can all be served from cache;
    final Enumerator<Object> cacheScan = Linq4j.iterableEnumerator(cacheResult.cachedRightRows);
    if (cacheResult.cacheMisses.isEmpty()) {
      return new AbstractEnumerable<Object>() {
        @Override
        public Enumerator<Object> enumerator() {
          return cacheScan;

        }
      };
    }
    final Set<List<CalciteKuduPredicate>> pushDownPredicates = cacheResult.cacheMisses.stream()
        .map(s -> rowTranslators.stream().map(t -> t.toPredicate((Object[]) s)).collect(Collectors.toList()))
        .collect(Collectors.toSet());

    final KuduEnumerable clone = rootEnumerable.clone(new LinkedList<>(pushDownPredicates));

    final Enumerable<Object> rightScan = new AbstractEnumerable<Object>() {
      @Override
      public Enumerator<Object> enumerator() {
        return new CacheAndScanEnumerator(cacheScan, clone.enumerator(), rowCache);
      }
    };

    return rightScan;
  }

  class CacheAndScanEnumerator implements Enumerator<Object> {

    final Enumerator<Object> hits;
    final Enumerator<Object> scan;
    final KuduRowCache rowCache;
    boolean cacheFirst = true;

    CacheAndScanEnumerator(final Enumerator<Object> hits, final Enumerator<Object> scan, final KuduRowCache rowCache) {
      this.scan = scan;
      this.hits = hits;
      this.rowCache = rowCache;
    }

    @Override
    public Object current() {
      if (cacheFirst) {
        return hits.current();
      } else {
        return scan.current();
      }
    }

    @Override
    public boolean moveNext() {
      if (cacheFirst) {
        cacheFirst = hits.moveNext();
      }
      if (cacheFirst) {
        return cacheFirst;
      } else {
        final boolean scanSuccessful = scan.moveNext();
        if (scanSuccessful) {
          rowCache.addToCache(scan.current());
        }
        return scanSuccessful;
      }
    }

    @Override
    public void reset() {
      hits.reset();
      scan.reset();

    }

    @Override
    public void close() {
      hits.close();
      scan.close();

    }

  }
}
