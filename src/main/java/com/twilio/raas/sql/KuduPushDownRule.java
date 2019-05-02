package com.twilio.raas.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.rex.RexCall;
import org.apache.kudu.ColumnSchema;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexDynamicParam;
import java.util.Optional;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexNode;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.nio.ByteBuffer;

import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KuduPushDownRule returns a List of a List of CalciteKuduPredicates. The inner list represents
 * a {@link org.apache.kudu.client.AsyncKuduScanner} that can be executed independently of each other. The outer 
 * list therefore represents a List of {@code AsyncKuduScanner}s that will satifisy the 
 * provided filters.
 *
 * It is expected that it is called with {@link RexNode} that represent the filters
 */
public class KuduPushDownRule implements RexBiVisitor<List<List<CalciteKuduPredicate>>, RexCall> {
    static final Logger logger = LoggerFactory
        .getLogger(KuduPushDownRule.class);

    private boolean allExpressionsConverted = true;
    private List<List<CalciteKuduPredicate>> startingScan;
    private Schema kuduTableSchema;

    public KuduPushDownRule(List<List<CalciteKuduPredicate>> startingScan, Schema kuduTableSchema) {
        this.startingScan = startingScan;
        this.kuduTableSchema = kuduTableSchema;
    }

    public boolean areAllFiltersApplied() {
        return allExpressionsConverted || true;
    }

    private List<List<CalciteKuduPredicate>> mergeBoolean(SqlKind booleanOp,
                                                          List<List<CalciteKuduPredicate>> left,
                                                          List<List<CalciteKuduPredicate>> right) {
        // happens only on the reduce call at the start.
        if (left == null) {
            return right;
        }
        switch(booleanOp) {
        case AND:
            if (left.isEmpty()) {
                return right;
            }
            if (right.isEmpty()) {
                return left;
            }
            // Merge both left and right together into one list. Every predicate in left and every predicate
            // in right must be applied in the scan.
            final ArrayList<List<CalciteKuduPredicate>> mergedPredicates = new ArrayList<>();
            for (List<CalciteKuduPredicate> leftPredicates: left) {
                for(List<CalciteKuduPredicate> rightPredicates: right) {
                    List<CalciteKuduPredicate> innerList = new ArrayList<>();
                    innerList.addAll(leftPredicates);
                    innerList.addAll(rightPredicates);
                    mergedPredicates.add(innerList);
                }
            }
            return mergedPredicates;
        case OR:
            // If there is no predicates in Left or Right, that means we are unable to push down
            // the entire scan tokens into Kudu. We cannot apply the right either in that case
            // so need to create an empty scan.
            if (left.isEmpty()) {
                // @TODO: mark this as un-filter-able
                return setEmpty();
            }
            if (right.isEmpty()) {
                // @TODO: mark this as un-filter-able
                return setEmpty();
            }

            // In this case, we have a set of predicates on the left, that will map to one
            // set of KuduScanTokens.
            // And we have a set of predicates on the right, that too will map to it's own set
            // of KuduScanTokens
            final ArrayList<List<CalciteKuduPredicate>> combined = new ArrayList<>();
            for (List<CalciteKuduPredicate> leftPredicates: left) {
                combined.add(leftPredicates);
            }
            for (List<CalciteKuduPredicate> rightPredicates: right) {
                combined.add(rightPredicates);
            }
            return combined;
        default:
            throw new IllegalArgumentException(String.format("Passed in a SqlKind operation that isn't supported: %s",
                                                             booleanOp));
        }
    }

    /**
     * A sql function call, process it. Including handling boolean 
     * calls.
     *
     * @param call  this is the relational call object to process
     *
     * @return updated list of kudu predicates
     */
    public List<List<CalciteKuduPredicate>> visitCall(RexCall call, RexCall parent) {
        final SqlKind callType = call.getOperator().getKind();

        switch (callType) {
        case EQUALS:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
            return call.operands.get(1).accept(this, call);
        case OR:
        case AND:
            return call.operands
                .stream()
                .map(rexNode -> rexNode.accept(this, call))
                .reduce(
                        null,
                        (left, right) -> mergeBoolean(callType, left, right));
        case NOT:
            if (call.operands.get(0) instanceof RexInputRef) {
                RexInputRef falseColumn = (RexInputRef) call.operands.get(0);
                final ColumnSchema columnSchema = this.kuduTableSchema.getColumnByIndex(falseColumn.getIndex());
                return Collections
                    .singletonList(Collections
                                   .singletonList(new CalciteKuduPredicate(columnSchema.getName(), KuduPredicate.ComparisonOp.EQUAL, Boolean.FALSE)));
            }
        }
        return setEmpty();
    }

    private Optional<KuduPredicate.ComparisonOp> findKuduOp(RexCall functionCall) {
        final SqlKind callType = functionCall.getOperator().getKind();
        switch(callType) {
        case EQUALS:
            return Optional.of(KuduPredicate.ComparisonOp.EQUAL);
        case GREATER_THAN:
            return Optional.of(KuduPredicate.ComparisonOp.GREATER);
        case GREATER_THAN_OR_EQUAL:
            return Optional.of(KuduPredicate.ComparisonOp.GREATER_EQUAL);
        case LESS_THAN:
            return Optional.of(KuduPredicate.ComparisonOp.LESS);
        case LESS_THAN_OR_EQUAL:
            return Optional.of(KuduPredicate.ComparisonOp.LESS_EQUAL);
        }
        return Optional.empty();
    }

    public List<List<CalciteKuduPredicate>> visitInputRef(RexInputRef inputRef, RexCall parent) {
        final ColumnSchema columnSchema = this.kuduTableSchema.getColumnByIndex(inputRef.getIndex());
        return Collections
            .singletonList(Collections
                           .singletonList(new CalciteKuduPredicate(columnSchema.getName(), KuduPredicate.ComparisonOp.EQUAL, Boolean.TRUE)));
    }

    public List<List<CalciteKuduPredicate>> visitLocalRef(RexLocalRef localRef, RexCall parent) {
        return setEmpty();
    }

    /**
     * This visit method adds a predicate. this is the leaf of a tree so it 
     * gets to create a fresh list of list
     */
    public List<List<CalciteKuduPredicate>> visitLiteral(RexLiteral literal, RexCall parent) {
        if (parent != null) {
            Optional<KuduPredicate.ComparisonOp> maybeOp = findKuduOp(parent);
            final RexNode left = parent.operands.get(0);
            if (left.getKind() == SqlKind.INPUT_REF) {
                final ColumnSchema columnSchema = this.kuduTableSchema.getColumnByIndex(((RexInputRef) left).getIndex());
                switch(literal.getType().getSqlTypeName()) {
                case NULL:
                    // maybeOp should be empty. but this is explicit.
                    return Collections.singletonList(Collections.singletonList(new CalciteKuduPredicate(columnSchema.getName(), null, null)));
                case BOOLEAN:
                    return Collections.singletonList(Collections.singletonList(new CalciteKuduPredicate(columnSchema.getName(), maybeOp.get(), (Boolean) literal.getValue2())));
                case DECIMAL:
                    // Ok so an long is coming into this spot.
                    if (literal.getValue2() instanceof BigDecimal) {
                        return Collections.singletonList(Collections.singletonList(new CalciteKuduPredicate(columnSchema.getName(), maybeOp.get(), (BigDecimal) literal.getValue2())));
                    }
                    else if (literal.getValue2() instanceof Long ||literal.getValue2() instanceof Double) {
                        return Collections.singletonList(Collections.singletonList(new CalciteKuduPredicate(columnSchema.getName(), maybeOp.get(), BigDecimal.valueOf(((Number)literal.getValue2()).doubleValue()))));
                    }
                    break;
                case DOUBLE:
                    return Collections.singletonList(Collections.singletonList(new CalciteKuduPredicate(columnSchema.getName(), maybeOp.get(), ((Number) literal.getValue2()).doubleValue())));
                case FLOAT:
                    return Collections.singletonList(Collections.singletonList(new CalciteKuduPredicate(columnSchema.getName(), maybeOp.get(), ((Number) literal.getValue2()).floatValue())));
                case TIMESTAMP:
                    // multiplied by 1000 as TIMESTAMP is in milliseconds and Kudu want's microseconds.
                    return Collections.singletonList(Collections.singletonList(new CalciteKuduPredicate(columnSchema.getName(), maybeOp.get(), ((Number) literal.getValue2()).longValue() * 1000)));
                case CHAR:
                case VARCHAR:
                    return Collections.singletonList(Collections.singletonList(new CalciteKuduPredicate(columnSchema.getName(), maybeOp.get(), (String) literal.getValue2())));
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                    return Collections.singletonList(Collections.singletonList(new CalciteKuduPredicate(columnSchema.getName(), maybeOp.get(), ((Number) literal.getValue2()).intValue())));
                case BIGINT:
                    return Collections.singletonList(Collections.singletonList(new CalciteKuduPredicate(columnSchema.getName(), maybeOp.get(), ((Number) literal.getValue2()).longValue())));
                case BINARY:
                    return Collections.singletonList(Collections.singletonList(new CalciteKuduPredicate(columnSchema.getName(), maybeOp.get(), (((ByteBuffer) literal.getValue2()).array()))));
                    
                }
            }	
        }
        return setEmpty();
    }

    public List<List<CalciteKuduPredicate>> visitOver(RexOver over, RexCall parent) {
        return setEmpty();
    }

    public List<List<CalciteKuduPredicate>> visitCorrelVariable(RexCorrelVariable correlVariable, RexCall parent) {
        return setEmpty();
    }

    public List<List<CalciteKuduPredicate>> visitDynamicParam(RexDynamicParam dynamicParam, RexCall parent) {
        return setEmpty();
    }

    public List<List<CalciteKuduPredicate>> visitRangeRef(RexRangeRef rangeRef, RexCall parent) {
        return setEmpty();
    }

    public List<List<CalciteKuduPredicate>> visitFieldAccess(RexFieldAccess fieldAccess, RexCall parent) {
        return setEmpty();
    }

    public List<List<CalciteKuduPredicate>> visitSubQuery(RexSubQuery subQuery, RexCall parent) {
        return setEmpty();
    }

    public List<List<CalciteKuduPredicate>> visitTableInputRef(RexTableInputRef ref, RexCall parent) {
        return setEmpty();
    }

    public List<List<CalciteKuduPredicate>> visitPatternFieldRef(RexPatternFieldRef ref, RexCall parent) {
        return setEmpty();
    }

    private List<List<CalciteKuduPredicate>> setEmpty() {
        allExpressionsConverted = false;
        return Collections.emptyList();
    }
}
