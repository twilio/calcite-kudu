package com.twilio.raas.sql.rules;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;

/**
 * Transforms row expressions being compared with a GREATER THAN operator
 *
 *  (A, B, C) >  (X, Y, Z)
 *   is transformed to
 *  (A > X)
 *   OR ((A = X) AND (B > Y))
 *   OR ((A = X) AND (B = Y) AND (C > Z))
 */
public class RowValueExpressionConverter  extends RexShuttle {

    private final RexBuilder rexBuilder;

    public RowValueExpressionConverter(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        if (!SqlKind.COMPARISON.contains(call.getKind()) || call.operands.size() != 2) {
            // require comparing two expressions
            return super.visitCall(call);
        }
        if (!(call.operands.get(0) instanceof RexCall) || !(call.operands.get(1) instanceof RexCall)) {
            // a row expression is of type RexCall
            return super.visitCall(call);
        }
        final RexCall op1 = (RexCall)call.operands.get(0);
        final RexCall op2 = (RexCall)call.operands.get(1);
        if (op2.getOperator().getKind() != SqlKind.ROW || op2.getOperator().getKind() != SqlKind.ROW) {
            // ensure both operands are row expressions
            return super.visitCall(call);
        }
        List<RexNode> rowExpr1 = op1.getOperands();
        List<RexNode> rowExpr2 = op2.getOperands();
        if (rowExpr1.size() != rowExpr2.size()) {
            // ensure both row expressions have the same number of operands
            return super.visitCall(call);
        }
        switch (call.getKind()) {
            case GREATER_THAN:
                List<RexNode> orNodes = Lists.newArrayListWithExpectedSize(rowExpr1.size());
                for (int i=0; i<rowExpr1.size(); ++i) {
                    List<RexNode> andNodes = Lists.newArrayListWithExpectedSize(i+1);
                    // create the equals column value comparison nodes
                    for (int j=0; j<i; ++j) {
                        RexNode equalNode = rexBuilder.makeCall(call.getType(),
                                SqlStdOperatorTable.EQUALS, ImmutableList.of(rowExpr1.get(j),
                                        rowExpr2.get(j)));
                        andNodes.add(equalNode);
                    }
                    // create the single greater than column value comparison node
                    RexNode greaterThanNode = rexBuilder.makeCall(call.getType(),
                            SqlStdOperatorTable.GREATER_THAN, ImmutableList.of(rowExpr1.get(i),
                                    rowExpr2.get(i)));
                    andNodes.add(greaterThanNode);
                    // create an AND'ed expression
                    RexNode and = RexUtil.composeConjunction(rexBuilder, andNodes);
                    orNodes.add(and);
                }
                // create an OR'ed expression
                return RexUtil.composeDisjunction(rexBuilder, orNodes);
            // TODO handle other types of comparison operators
            default:
                return super.visitCall(call);
        }
    }

}
