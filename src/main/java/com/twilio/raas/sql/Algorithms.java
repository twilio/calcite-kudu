package com.twilio.raas.sql;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;

public final class Algorithms {
    /**
     * Implementation of Nested Loop Join for Left and INNER join. The Left side is the driver. For
     * batchSize records on the left, push the join keys from the left into a KuduPredicate on the right.
     *
     * @param joinType Join type being executed. Must be {@link JoinType#INNER} or {@link JoinType#LEFT}
     * @param left The left side of the join
     * @param right a function that produces a new right enumerable. Should be generated
     *        from {@link SortableEnumerable#nestedJoinPredicates}
     * @param resultSelector function that combines a row from the left and a row from the right
     * @param predicate function that takes both left and right and returns true if their keys match.
     *
     * @return an {@link Enumerable} that results in the join.
     */
    public static <TSource, TRight, TResult> Enumerable<TResult> correlateBatchJoin(final JoinType joinType,
            final Enumerable<TSource> left, final Function1<List<TSource>, Enumerable<TRight>> right,
            final Function2<TSource, TRight, TResult> resultSelector, final Predicate2<TSource, TRight> predicate,
            final int batchSize) {
        if (joinType != JoinType.INNER && joinType != JoinType.LEFT) {
            throw new IllegalArgumentException("Nested loop join cannot be applied to join of type: " + joinType);
        }
        return new AbstractEnumerable<TResult>() {
            @Override
            public Enumerator<TResult> enumerator() {
                return new Enumerator<TResult>() {
                    Enumerator<TSource> leftEnumerator = left.enumerator();
                    List<TSource> leftValues = new ArrayList<>(batchSize);
                    List<TRight> rightValues = new ArrayList<>();
                    TSource leftValue;
                    TRight rightValue;
                    Enumerable<TRight> rightEnumerable;
                    Enumerator<TRight> rightEnumerator;
                    boolean rightEnumHasNext = false;
                    boolean atLeastOneResult = false;
                    int i = -1; // left position
                    int j = -1; // right position

                    @Override
                    public TResult current() {
                        return resultSelector.apply(leftValue, rightValue);
                    }

                    @Override
                    public boolean moveNext() {
                        while (true) {
                            // Fetch a new batch
                            if (i == leftValues.size() || i == -1) {
                                i = 0;
                                j = 0;
                                leftValues.clear();
                                rightValues.clear();
                                atLeastOneResult = false;
                                while (leftValues.size() < batchSize && leftEnumerator.moveNext()) {
                                    TSource tSource = leftEnumerator.current();
                                    leftValues.add(tSource);
                                }
                                if (leftValues.isEmpty()) {
                                    return false;
                                }
                                rightEnumerable = right.apply(leftValues);

                                if (rightEnumerable == null) {
                                    rightEnumerable = Linq4j.emptyEnumerable();
                                }
                                rightEnumerator = rightEnumerable.enumerator();
                                rightEnumHasNext = rightEnumerator.moveNext();


                                do {
                                    if (rightEnumerator.current() != null) {
                                        rightValues.add(rightEnumerator.current());
                                    }
                                } while (rightEnumerator.moveNext());
                            }

                            // Populated leftValues at batch size and populated rightValues
                            leftValue = leftValues.get(i);
                            rightValue = null;

                            if (!rightValues.isEmpty()) {
                                while (j < rightValues.size()) {
                                    if (predicate.apply(leftValue, rightValues.get(j))) {
                                        atLeastOneResult = true;
                                        rightValue = rightValues.get(j);
                                        if (joinType == JoinType.ANTI || joinType == JoinType.SEMI) {
                                            // @TODO: how to support these?
                                        }
                                        // Don't move i -- there might be more matching on the right.
                                        j++;
                                        return true;
                                    }
                                    j++;
                                }
                            }

                            // Didn't find a record on the right.
                            if (joinType == JoinType.LEFT && !atLeastOneResult) {
                                // @TODO: what is the right hand value here.
                                i++;
                                j = 0;
                                return true;
                            } else if (atLeastOneResult && (joinType == JoinType.LEFT || joinType == JoinType.INNER)) {
                                i++;
                                j = 0;
                                atLeastOneResult = false;
                            } else {
                                return false;
                            }
                        }
                    }

                    @Override
                    public void reset() {
                        leftEnumerator.reset();
                        rightValue = null;
                        leftValue = null;
                        leftValues.clear();
                        rightValues.clear();
                        atLeastOneResult = false;
                        i = -1;
                    }

                    @Override
                    public void close() {
                        leftEnumerator.close();
                        if (rightEnumerator != null) {
                            rightEnumerator.close();
                        }
                        leftValue = null;
                        rightValue = null;
                    }
                };
            }
        };
    }
}
