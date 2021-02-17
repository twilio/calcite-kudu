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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.mockito.ArgumentMatchers.any;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.junit.Test;

public final class NestedJoinCacheTest {
  @Test
  public void cacheTest() {
    final List<TranslationPredicate> joinTranslations = Collections
        .singletonList(new TranslationPredicate(1, 0, ComparisonOp.EQUAL));
    final CloneableEnumerable<Object> mockBaseEnumerable = mock(CloneableEnumerable.class);
    final NestedJoinFactory joinFactory = new NestedJoinFactory(10, joinTranslations, mockBaseEnumerable);
    final CloneableEnumerable<Object> mockEnumerableFirstRow = mock(CloneableEnumerable.class);
    final CloneableEnumerable<Object> mockEnumerableSecondRow = mock(CloneableEnumerable.class);
    final CloneableEnumerable<Object> mockEnumerableThirdRow = mock(CloneableEnumerable.class);
    final List<Object> firstBatch = Arrays.asList(new Object[] { "Awesome", Integer.valueOf(1) },
        new Object[] { "Awesome2", Integer.valueOf(2) }, new Object[] { "Awesome3", Integer.valueOf(3) });

    List<List<CalciteKuduPredicate>> firstRowPredicates = Collections
        .singletonList(Arrays.asList(new ComparisonPredicate(0, ComparisonOp.EQUAL, Integer.valueOf(1))));
    List<List<CalciteKuduPredicate>> secondRowPredicates = Collections
        .singletonList(Arrays.asList(new ComparisonPredicate(0, ComparisonOp.EQUAL, Integer.valueOf(2))));
    List<List<CalciteKuduPredicate>> thirdRowPredicates = Collections
        .singletonList(Arrays.asList(new ComparisonPredicate(0, ComparisonOp.EQUAL, Integer.valueOf(3))));

    when(mockBaseEnumerable.clone(firstRowPredicates)).thenReturn(mockEnumerableFirstRow);
    when(mockBaseEnumerable.clone(secondRowPredicates)).thenReturn(mockEnumerableSecondRow);
    when(mockBaseEnumerable.clone(thirdRowPredicates)).thenReturn(mockEnumerableThirdRow);

    when(mockEnumerableFirstRow.enumerator()).thenAnswer(
        x -> new ListEnumerator<Object>(Collections.singletonList(new Object[] { Integer.valueOf(1), "OneRow" })));
    when(mockEnumerableSecondRow.enumerator()).thenAnswer(
        x -> new ListEnumerator<Object>(Collections.singletonList(new Object[] { Integer.valueOf(2), "TwoRow" })));
    when(mockEnumerableThirdRow.enumerator()).thenAnswer(
        x -> new ListEnumerator<Object>(Collections.singletonList(new Object[] { Integer.valueOf(3), "ThirdRow" })));

    // Run the same rows through a bunch of times.
    for (int i = 0; i < 1000; i++) {
      final List<Object> rightHandSide = joinFactory.apply(firstBatch).toList();

      assertEquals(String.format("Right hand side should return three rows it returned %d", rightHandSide.size()), 3,
          rightHandSide.size());
    }

    // Verify that enumerator was called only once for each row.
    Arrays.asList(mockEnumerableFirstRow, mockEnumerableSecondRow, mockEnumerableThirdRow)
        .forEach(rowEnumerable -> verify(rowEnumerable, times(1)).enumerator());

    // Test to make sure we created a clone only 3 times.
    verify(mockBaseEnumerable, times(3)).clone(any());
  }

  @Test
  public void smallCache() {
    final List<TranslationPredicate> joinTranslations = Collections
        .singletonList(new TranslationPredicate(1, 0, ComparisonOp.EQUAL));
    final CloneableEnumerable<Object> mockBaseEnumerable = mock(CloneableEnumerable.class);
    final NestedJoinFactory joinFactory = new NestedJoinFactory(2, joinTranslations, mockBaseEnumerable);
    final CloneableEnumerable<Object> mockEnumerableFirstRow = mock(CloneableEnumerable.class);
    final CloneableEnumerable<Object> mockEnumerableSecondRow = mock(CloneableEnumerable.class);
    final CloneableEnumerable<Object> mockEnumerableThirdRow = mock(CloneableEnumerable.class);
    final List<Object> firstBatch = Arrays.asList(new Object[] { "Awesome", Integer.valueOf(1) },
        new Object[] { "Awesome2", Integer.valueOf(2) }, new Object[] { "Awesome3", Integer.valueOf(3) });

    List<List<CalciteKuduPredicate>> firstRowPredicates = Collections
        .singletonList(Arrays.asList(new ComparisonPredicate(0, ComparisonOp.EQUAL, Integer.valueOf(1))));
    List<List<CalciteKuduPredicate>> secondRowPredicates = Collections
        .singletonList(Arrays.asList(new ComparisonPredicate(0, ComparisonOp.EQUAL, Integer.valueOf(2))));
    List<List<CalciteKuduPredicate>> thirdRowPredicates = Collections
        .singletonList(Arrays.asList(new ComparisonPredicate(0, ComparisonOp.EQUAL, Integer.valueOf(3))));

    when(mockBaseEnumerable.clone(firstRowPredicates)).thenReturn(mockEnumerableFirstRow);
    when(mockBaseEnumerable.clone(secondRowPredicates)).thenReturn(mockEnumerableSecondRow);
    when(mockBaseEnumerable.clone(thirdRowPredicates)).thenReturn(mockEnumerableThirdRow);

    when(mockEnumerableFirstRow.enumerator()).thenAnswer(
        x -> new ListEnumerator<Object>(Collections.singletonList(new Object[] { Integer.valueOf(1), "OneRow" })));
    when(mockEnumerableSecondRow.enumerator()).thenAnswer(
        x -> new ListEnumerator<Object>(Collections.singletonList(new Object[] { Integer.valueOf(2), "TwoRow" })));
    when(mockEnumerableThirdRow.enumerator()).thenAnswer(
        x -> new ListEnumerator<Object>(Collections.singletonList(new Object[] { Integer.valueOf(3), "ThirdRow" })));

    // Run the same rows through a bunch of times.
    for (int i = 0; i < 3; i++) {
      final List<Object> rightHandSide = joinFactory.apply(firstBatch).toList();
      // Prime the second row in the cache to keep it up front.
      joinFactory.apply(Collections.singletonList(firstBatch.get(1)));

      assertEquals(String.format("Right hand side should return three rows it returned %d", rightHandSide.size()), 3,
          rightHandSide.size());
    }

    verify(mockEnumerableFirstRow, times(3)).enumerator();
    verify(mockEnumerableSecondRow, times(1)).enumerator();
    verify(mockEnumerableThirdRow, times(3)).enumerator();

    verify(mockBaseEnumerable, times(3)).clone(firstRowPredicates);
    verify(mockBaseEnumerable, times(3)).clone(thirdRowPredicates);
    verify(mockBaseEnumerable, times(1)).clone(secondRowPredicates);
  }

  class ListEnumerator<T> implements Enumerator<T> {

    int i = -1;
    final List<T> results;
    boolean closed = false;

    ListEnumerator(final List<T> results) {
      this.results = results;
    }

    @Override
    public T current() {
      return results.get(i);
    }

    @Override
    public boolean moveNext() {
      if (closed) {
        throw new AssertionError("Source enumerable has already been closed and it shouldn't be reopened");
      }
      return ++i < results.size();
    }

    @Override
    public void reset() {
      i = -1;
    }

    @Override
    public void close() {
      closed = true;
    }

  }
}
