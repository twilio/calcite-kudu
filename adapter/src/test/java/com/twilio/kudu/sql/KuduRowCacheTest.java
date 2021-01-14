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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import com.twilio.kudu.sql.KuduRowCache.SearchResult;

import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Predicate2;
import org.junit.Test;

public class KuduRowCacheTest {

  private EqualityComparer<Object> cacheEqualCheck = new EqualityComparer<Object>() {
    @Override
    public boolean equal(Object v1, Object v2) {
      return v1.equals(v2);
    }

    @Override
    public int hashCode(Object t) {
      return t.hashCode();
    }
  };

  private Predicate2<Object, Object> joinMatches = (v0, v1) -> v0.equals(v1);

  @Test
  public void emptyCache() {

    final List<Object> leftBatch = Arrays.asList("account1", "account2");
    final KuduRowCache cache = new KuduRowCache(cacheEqualCheck, 10);
    final SearchResult cacheResult = cache.cacheSearch(leftBatch, joinMatches);

    assertTrue(String.format("Cache hits should be empty: %s", cacheResult.cachedRightRows),
        cacheResult.cachedRightRows.isEmpty());
    assertEquals(String.format("Cache misses should match the search: %s", cacheResult.cacheMisses), leftBatch,
        cacheResult.cacheMisses);
  }

  @Test
  public void singleMatch() {

    final List<Object> leftBatch = Arrays.asList("account1", "account2", "account3", "account4", "account5");
    final KuduRowCache cache = new KuduRowCache(cacheEqualCheck, 10);
    cache.addToCache("account3");
    cache.addToCache("account4");
    cache.addToCache("account2");
    cache.addToCache("account5");
    cache.addToCache("account6");
    cache.addToCache("account10");

    final SearchResult cacheResult = cache.cacheSearch(leftBatch, joinMatches);

    assertEquals(
        String.format("Cache result should have a cache matches for account2 - 5: %s", cacheResult.cachedRightRows),
        Arrays.asList("account5", "account2", "account4", "account3"), cacheResult.cachedRightRows);
    assertEquals(String.format("Cache misses contain only account1", cacheResult.cacheMisses),
        Arrays.asList("account1"), cacheResult.cacheMisses);
  }

  @Test
  public void noMatchingJoinCondition() {

    final List<Object> leftBatch = Arrays.asList("account1", "account2");
    final KuduRowCache cache = new KuduRowCache(cacheEqualCheck, 10);
    cache.addToCache("account1235");
    cache.addToCache("account12389");
    cache.addToCache("account2");

    final SearchResult cacheResult = cache.cacheSearch(leftBatch, Predicate2.FALSE);

    assertTrue(String.format("Cache hits should be empty: %s", cacheResult.cachedRightRows),
        cacheResult.cachedRightRows.isEmpty());
    assertEquals(String.format("Cache misses should match the search: %s", cacheResult.cacheMisses), leftBatch,
        cacheResult.cacheMisses);
  }

  @Test
  public void cachedRowEvicted() {

    final List<Object> leftBatch = Arrays.asList("account1", "account2");
    final KuduRowCache cache = new KuduRowCache(cacheEqualCheck, 2);
    cache.addToCache("account2");
    cache.addToCache("account1235");
    cache.addToCache("account12389");

    final SearchResult cacheResult = cache.cacheSearch(leftBatch, joinMatches);

    assertTrue(String.format("Cache hits should be empty: %s", cacheResult.cachedRightRows),
        cacheResult.cachedRightRows.isEmpty());
    assertEquals(String.format("Cache misses should match the search: %s", cacheResult.cacheMisses), leftBatch,
        cacheResult.cacheMisses);
  }
}
