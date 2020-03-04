package com.twilio.raas.sql;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;

/**
 * A collection of static implementations of SQL algorithms.
 */
public final class Algorithms {
    /**
     * An implementation of nested loop join
     */
    public static <TSource, TInner, TResult> Enumerable<TResult> nestedLoopJoinOptimized(
            final Enumerable<TSource> outer, final Enumerable<TInner> inner,
            final Function2<TSource, Object, TResult> resultSelector, final Join join) {
        final JoinRelType joinType = join.getJoinType();
        if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
            throw new IllegalArgumentException("JoinType " + joinType + " is unsupported");
        }

        if (!(inner instanceof SortableEnumerable)) {
            throw new IllegalArgumentException("Kudu's Nested Loop Join requires a Kudu Enumerable on the Right");
        }

        final SortableEnumerable original = ((SortableEnumerable) inner);
        final ConditionTranslationVisitor findConditions =
            new ConditionTranslationVisitor(
                join.getLeft().getRowType().getFieldCount(), original.getTableSchema());
        final List<TranslationPredicate> translatablePredicates = join.getCondition()
                .accept(findConditions);

        return new AbstractEnumerable<TResult>() {
            public Enumerator<TResult> enumerator() {
                return new Enumerator<TResult>() {
                    private Enumerator<TSource> outerEnumerator = outer.enumerator();
                    private Enumerator<TInner> innerEnumerator = null;
                    private TSource outerValue;
                    private Object innerValue;
                    private int state = 0;
                    private boolean outerMatch = false;

                    @Override
                    public TResult current() {
                        return resultSelector.apply(outerValue, innerValue);
                    }

                    @Override
                    public boolean moveNext() {
                        while (true) {
                            switch (state) {
                            case 0:
                                if (!outerEnumerator.moveNext()) {
                                    return false;
                                }

                                outerValue = outerEnumerator.current();
                                outerMatch = false;
                                state = 1;

                                // Calculate the new predicates for the inner based on outerValue
                                final List<KuduPredicate> additionalPredicates = translatablePredicates
                                    .stream()
                                    .map(t -> t.toPredicate((Object[]) outerValue))
                                    .collect(Collectors.toList());

                                final SortableEnumerable clonedEnumerable = original.clone(additionalPredicates);
                                final Enumerator<Object> innerEnumerator = clonedEnumerable.enumerator();
                                continue;
                            case 1:
                                if (innerEnumerator.moveNext()) {
                                    innerValue = innerEnumerator.current();
                                    outerMatch = true;
                                    switch (joinType) {
                                    case ANTI:
                                        state = 0;
                                        continue;
                                    case SEMI:
                                        state = 0;
                                        return true;
                                    case INNER:
                                    case LEFT:
                                        return true;
                                    }
                                }
                                else {
                                    state = 0;
                                    innerValue = null;
                                    if (!outerMatch
                                        && (joinType == JoinRelType.LEFT || joinType == JoinRelType.ANTI)) {
                                        return true;
                                    }
                                }
                            }
                        }
                    }

                    @Override
                    public void reset() {
                        state = 0;
                        outerMatch = false;
                        outerEnumerator.reset();
                    }

                    @Override
                    public void close() {
                        outerEnumerator.close();
                    }
                };
            }
        };
    }


    /**
     * Translates a RexCall with two RexInputRefs into a predicate based on the LEFT row.
     */
    public static class TranslationPredicate {
        private final int leftKuduIndex;
        private final int rightKuduIndex;
        private final ComparisonOp operation;
        private final SqlTypeName type;
        private final Schema tableSchema;

        public TranslationPredicate(final int leftOrdinal, final int rightOrdinal,
            final RexCall functionCall, final SqlTypeName type, final Schema tableSchema) {
            this.leftKuduIndex = leftOrdinal;
            this.rightKuduIndex = rightOrdinal;
            this.type = type;
            this.tableSchema = tableSchema;

            switch (functionCall.getOperator().getKind()) {
            case EQUALS:
                this.operation = ComparisonOp.EQUAL;
                break;
            case GREATER_THAN:
                this.operation = ComparisonOp.GREATER;
                break;
            case GREATER_THAN_OR_EQUAL:
                this.operation = ComparisonOp.GREATER_EQUAL;
                break;
            case LESS_THAN:
                this.operation = ComparisonOp.LESS;
                break;
            case LESS_THAN_OR_EQUAL:
                this.operation = ComparisonOp.LESS_EQUAL;
            default:
                throw new IllegalArgumentException(
                    String.format("TranslationPredicate is unable to handle this call type: %s",
                        functionCall.getOperator().getKind()));
            }
        }

        public KuduPredicate toPredicate(final Object[] leftRow) {
            // @TODO: How to handle descendingSorted fields?
            switch(type) {
            case BOOLEAN:
                return KuduPredicate
                    .newComparisonPredicate(
                        tableSchema.getColumnByIndex(rightKuduIndex),
                        operation,
                        ((Boolean) leftRow[leftKuduIndex]));
            case INTEGER:
            case TINYINT:
            case SMALLINT:
                return KuduPredicate
                    .newComparisonPredicate(
                        tableSchema.getColumnByIndex(rightKuduIndex),
                        operation,
                        ((Integer) leftRow[leftKuduIndex]));
            case DECIMAL:
                return KuduPredicate
                    .newComparisonPredicate(
                        tableSchema.getColumnByIndex(rightKuduIndex),
                        operation,
                        ((BigDecimal) leftRow[leftKuduIndex]));
            case DOUBLE:
                return KuduPredicate
                    .newComparisonPredicate(
                        tableSchema.getColumnByIndex(rightKuduIndex),
                        operation,
                        ((Double) leftRow[leftKuduIndex]));
            case FLOAT:
                return KuduPredicate
                    .newComparisonPredicate(
                        tableSchema.getColumnByIndex(rightKuduIndex),
                        operation,
                        ((Float) leftRow[leftKuduIndex]));
            case TIMESTAMP:
                // @TODO: what is the type in this instance?
                return KuduPredicate
                    .newComparisonPredicate(
                        tableSchema.getColumnByIndex(rightKuduIndex),
                        operation,
                        ((TimestampString) leftRow[leftKuduIndex]));
            case CHAR:
            case VARCHAR:
                return KuduPredicate
                    .newComparisonPredicate(
                        tableSchema.getColumnByIndex(rightKuduIndex),
                        operation,
                        leftRow[leftKuduIndex].toString());
            default:
                throw new IllegalArgumentException(
                    String.format("Unable to create a Kudu Predicate for type %s",
                        type.toString()));

            }
        }
    }

    /**
     * Computes the conjunction based on the join condition
     */
    public static class ConditionTranslationVisitor extends RexVisitorImpl<List<TranslationPredicate>> {
        private int leftSize;
        private Schema tableSchema;
        public ConditionTranslationVisitor(final int leftSize, Schema tableSchema) {
            super(true);
            this.leftSize = leftSize;
            this.tableSchema = tableSchema;
        }

        public List<TranslationPredicate> visitCall(RexCall call) {
            final SqlKind callType = call.getOperator().getKind();

            switch (callType) {
            case EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                if (call.operands.get(0) instanceof RexInputRef &&
                    call.operands.get(1) instanceof RexInputRef) {
                    final RexInputRef left;
                    final RexInputRef right;
                    if (((RexInputRef) call.operands.get(0)).getIndex() <
                        ((RexInputRef) call.operands.get(1)).getIndex()) {
                        left = (RexInputRef) call.operands.get(0);
                        right = (RexInputRef) call.operands.get(1);
                    }
                    else {
                        left = (RexInputRef) call.operands.get(1);
                        right = (RexInputRef) call.operands.get(0);
                    }
                    return Collections
                        .singletonList(
                            new TranslationPredicate(
                                left.getIndex(),
                                right.getIndex() - leftSize,
                                call,
                                left.getType().getSqlTypeName(),
                                tableSchema));
                }
                else {
                    throw new IllegalArgumentException(
                        "Unable to construct a Kudu Predicate for join condition that doesn't contain two InputRefs");
                }

            case AND:
                return call.operands
                    .stream()
                    .map(rexNode -> rexNode.accept(this))
                    .reduce(
                        Collections.emptyList(),
                        (left, right) -> {
                            if (left.isEmpty()) {
                                return right;
                            }
                            if (right.isEmpty()) {
                                return left;
                            }

                            final ArrayList<TranslationPredicate> merged = new ArrayList<>();
                            merged.addAll(left);
                            merged.addAll(right);
                            return merged;
                        });
            default:
                throw new IllegalArgumentException(
                    String.format(
                        "Unable to Use Nested Loop Join with a this condition: %s call type",
                        callType));

        }
    }
}
