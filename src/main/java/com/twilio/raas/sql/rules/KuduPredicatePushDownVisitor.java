package com.twilio.raas.sql.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.twilio.raas.sql.CalciteKuduPredicate;
import com.twilio.raas.sql.ComparisonPredicate;
import com.twilio.raas.sql.InListPredicate;
import com.twilio.raas.sql.NullPredicate;

import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.type.SqlTypeName;
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
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexNode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KuduPredicatePushDownVisitor returns a List of a List of CalciteKuduPredicates. The inner list represents
 * a {@link org.apache.kudu.client.AsyncKuduScanner} that can be executed independently of each other. The outer
 * list therefore represents a List of {@code AsyncKuduScanner}s that will satifisy the
 * provided filters.
 *
 * It is expected that it is called with {@link RexNode} that represent the filters
 */
public class KuduPredicatePushDownVisitor implements RexBiVisitor<List<List<CalciteKuduPredicate>>, RexCall> {
    static final Logger logger = LoggerFactory
        .getLogger(KuduPredicatePushDownVisitor.class);

    private boolean allExpressionsConverted = true;

    /**
     * @return true if we can push down all filters to kudu
     */
    public boolean areAllFiltersApplied() {
        return allExpressionsConverted;
    }

    public static List<List<CalciteKuduPredicate>> mergePredicateLists(
        SqlKind booleanOp,
        List<List<CalciteKuduPredicate>>left,
        List<List<CalciteKuduPredicate>> right) {
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
                return Collections.emptyList();
            }
            if (right.isEmpty()) {
                return Collections.emptyList();
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

    private List<List<CalciteKuduPredicate>> mergeBoolean(SqlKind booleanOp,
                                                          List<List<CalciteKuduPredicate>> left,
                                                          List<List<CalciteKuduPredicate>> right) {
        // happens only on the reduce call at the start.
        if (left == null) {
            return right;
        }
        final List<List<CalciteKuduPredicate>> combined = mergePredicateLists(
            booleanOp, left, right);

        // If there is no predicates in Left or Right, that means we are unable to push down
        // the entire scan tokens into Kudu.
        if (booleanOp == SqlKind.OR && combined.isEmpty()) {
            return setEmpty();
        }
        return combined;
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
                return Collections
                    .singletonList(Collections
                                   .singletonList(new ComparisonPredicate(falseColumn.getIndex(),
                                           KuduPredicate.ComparisonOp.EQUAL, Boolean.FALSE)));
            }
        case IN:
          if (call.operands.get(0) instanceof RexInputRef) {
            if (call.operands.get(1) instanceof RexCall) {
              final RexCall right = (RexCall) call.operands.get(1);
              if (right.getKind() == SqlKind.ARRAY_VALUE_CONSTRUCTOR) {
                final boolean nonLiteral = right.getOperands()
                  .stream()
                  .filter(rexNode -> ! rexNode.isA(SqlKind.LITERAL))
                  .findAny()
                  .isPresent();

                // inPredicate only handles literals, it doesn't handle scans
                if (!nonLiteral) {
                  final List<Object> castedLiterals = right.getOperands().stream()
                    .map(l -> castLiteral((RexLiteral) l))
                    .collect(Collectors.toList());
                  final RexInputRef idx = (RexInputRef) call.operands.get(0);
                  return Collections.singletonList(
                      Collections.singletonList(
                          new InListPredicate(
                              idx.getIndex(), castedLiterals)));
                }
              }
            }
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
        return Collections
            .singletonList(Collections
                           .singletonList(new ComparisonPredicate(
                                inputRef.getIndex(), KuduPredicate.ComparisonOp.EQUAL, Boolean.TRUE)));
    }

    public List<List<CalciteKuduPredicate>> visitLocalRef(RexLocalRef localRef, RexCall parent) {
        return setEmpty();
    }

    private int getColumnIndex(RexNode node) {
      if (node instanceof RexInputRef) {
        return ((RexInputRef) node).getIndex();
      }
      // if the node is an expression of the form expr#14=[CAST($t3):INTEGER] we need to exptract
      // the column index from the input to the CAST expression
      else if (node instanceof RexCall) {
        RexCall call = (RexCall) node;
        if (call.getKind() == SqlKind.CAST) {
          return getColumnIndex(call.getOperands().get(0));
        }
      }
      throw new UnsupportedOperationException("Unable to determine column index from node " + node);
    }

  private Object castLiteral(RexLiteral literal) {
    switch(literal.getType().getSqlTypeName()) {
    case BOOLEAN:
      return RexLiteral.booleanValue(literal);
    case DECIMAL:
      return literal.getValueAs(BigDecimal.class);
    case DOUBLE:
      return literal.getValueAs(Double.class);
    case FLOAT:
      return literal.getValueAs(Float.class);
    case TIMESTAMP:
      // multiplied by 1000 as TIMESTAMP is in milliseconds and Kudu want's microseconds.
      return literal.getValueAs(Long.class) * 1000;
    case CHAR:
    case VARCHAR:
      return literal.getValueAs(String.class);
    case TINYINT:
    case SMALLINT:
    case INTEGER:
      return literal.getValueAs(Integer.class);
    case BIGINT:
      return literal.getValueAs(Long.class);
    case BINARY:
      return (((ByteBuffer) literal.getValue4()).array());
    }
    throw new IllegalArgumentException(String.format("Unable to cast literal to Kudu: %s", literal));
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
                final int index = getColumnIndex(left);

                if (literal.getType().getSqlTypeName() == SqlTypeName.NULL) {
                    return Collections.singletonList(Collections.singletonList(
                            new NullPredicate(index, false)));
                }
                // everything else requires op to be set.
                else if (!maybeOp.isPresent()) {
                    return setEmpty();
                }
                else {
                  return Collections.singletonList(
                      Collections.singletonList(
                          new ComparisonPredicate(
                              index,
                              maybeOp.get(),
                              castLiteral(literal)
                          )
                      )
                  );
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
