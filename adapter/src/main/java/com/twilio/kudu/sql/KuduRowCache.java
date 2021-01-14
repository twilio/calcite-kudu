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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Predicate2;

/**
 * {@code KuduRowCache} contains a fixed size cache for the right hand side of a
 * {@link com.twilio.kudu.sql.rel.KuduNestedJoin}
 */
public class KuduRowCache {
  private final LinkedList<CacheHit> cacheList;
  private final EqualityComparer<Object> comparer;
  private final int cacheSize;

  /**
   * Create a cache with the provided {@link EqualityComparer} for the cache and a
   * fixed size
   *
   * @param comparer  captures how to compare rows on the right hand side as equal
   *                  or not
   * @param cacheSize maximum size of the cache.
   */
  public KuduRowCache(final EqualityComparer<Object> comparer, final int cacheSize) {
    this.comparer = comparer;
    this.cacheList = new LinkedList<>();
    this.cacheSize = cacheSize;
  }

  /**
   * Search the cache for records that match the rows on the from the left table
   *
   * @param batchFromLeftTable rows fetched from the Left table to match
   * @param joinCondition      a function that compares a row from the left and a
   *                           row in the cache to see if it satisifies the join
   *                           condition
   *
   * @return {@link SearchResult} that contains both the hits and the misses.
   */
  public SearchResult cacheSearch(final List<Object> batchFromLeftTable,
      final Predicate2<Object, Object> joinCondition) {
    // @TODO: size estimation? We know it won't be larger than batchFromRight
    final ArrayList<Object> cacheMisses = new ArrayList<>();
    final ArrayList<Integer> indicesInCache = new ArrayList<>();

    final HashSet<CacheHit> hits = new HashSet<>();
    for (Object leftRow : batchFromLeftTable) {
      boolean foundInCache = false;
      for (int j = 0; j < cacheList.size(); j++) {
        final CacheHit cachedRightRow = cacheList.get(j);
        if (joinCondition.apply(leftRow, cachedRightRow.rowValue)) {
          // Keep track of cache shuffling for this row.
          if (hits.add(cachedRightRow)) {
            indicesInCache.add(j);
          }
          foundInCache = true;
          break;
        }
      }
      if (!foundInCache) {
        cacheMisses.add(leftRow);
      }
    }

    // @TODO: reshuffle the cache list and push the hits to the front.

    if (hits.isEmpty()) {
      return new SearchResult(Collections.emptyList(), cacheMisses);
    } else {
      return new SearchResult(hits.stream().map(h -> h.rowValue).collect(Collectors.toList()), cacheMisses);
    }
  }

  /**
   * Add the right row into the cache and ensure the cache doesn't grow to large
   *
   * @param rightRow the row from the right hand side of the join.
   */
  public void addToCache(final Object rightRow) {
    cacheList.push(new CacheHit(rightRow));
    if (cacheList.size() > cacheSize) {
      cacheList.pollLast();
    }
  }

  private class CacheHit {
    final Object rowValue;

    CacheHit(Object hit) {
      this.rowValue = hit;
    }

    @Override
    public boolean equals(final Object o) {
      if (o instanceof CacheHit) {
        return comparer.equal(rowValue, ((CacheHit) o).rowValue);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return comparer.hashCode(rowValue);
    }
  }

  public class SearchResult {
    /**
     * Rows from the in memory cache that match rows on the left.
     */
    List<Object> cachedRightRows;

    /**
     * Rows from the left table that do not match anything in the cache;
     */
    List<Object> cacheMisses;

    SearchResult(final List<Object> cachedRightRows, final List<Object> cacheMisses) {
      this.cachedRightRows = cachedRightRows;
      this.cacheMisses = cacheMisses;
    }
  }
}
