/* Copyright 2020 Twilio, Inc
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
package com.twilio.kudu.sql.rules;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.twilio.kudu.sql.CalciteKuduTable;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;

/**
 * Used to paginate through rows in the order of the primary key columns by
 * comparing row expressions with a greater than operator
 *
 * If the the PK of the table is
 * {@code (A asc, B asc, C asc) (A, B, C) > ('a1', 'b1', * 'c1')} is transformed
 * to
 * {@code (A > 'a1') OR ((A = 'a1') AND (B > 'b1')) OR ((A = 'a1') AND (B = 'b1') AND (C > 'c1'))}
 *
 * If the the PK of the table is
 * {@code (A asc, B desc, C asc) (A, B, C) > ('a1', 'b1', 'c1')} is transformed
 * to
 * {@code (A > 'a1') OR ((A = 'a1') AND (B < 'b1')) OR ((A = 'a1') AND (B = 'b1') AND (C > 'c1'))}
 *
 * This implementation is different from the SQL-92 standard see
 * https://stackoverflow.com/questions/32981903/sql-syntax-term-for-where-col1-col2-val1-val2/32982077#32982077
 */
public class RowValueExpressionConverter extends RexShuttle {

  private final RexBuilder rexBuilder;
  private final CalciteKuduTable calciteKuduTable;

  public RowValueExpressionConverter(RexBuilder rexBuilder, CalciteKuduTable calciteKuduTable) {
    this.rexBuilder = rexBuilder;
    this.calciteKuduTable = calciteKuduTable;
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
    final RexCall op1 = (RexCall) call.operands.get(0);
    final RexCall op2 = (RexCall) call.operands.get(1);
    if (op1.getOperator().getKind() != SqlKind.ROW || op2.getOperator().getKind() != SqlKind.ROW) {
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
      for (int i = 0; i < rowExpr1.size(); ++i) {
        List<RexNode> andNodes = Lists.newArrayListWithExpectedSize(i + 1);
        // create the equals column value comparison nodes
        for (int j = 0; j < i; ++j) {
          RexNode equalNode = rexBuilder.makeCall(call.getType(), SqlStdOperatorTable.EQUALS,
              ImmutableList.of(rowExpr1.get(j), rowExpr2.get(j)));
          andNodes.add(equalNode);
        }
        // create the single greater than column value comparison node
        RexInputRef rexInputRef = (RexInputRef) rowExpr1.get(i);
        SqlBinaryOperator operator = calciteKuduTable.isColumnOrderedDesc((rexInputRef.getIndex()))
            ? SqlStdOperatorTable.LESS_THAN
            : SqlStdOperatorTable.GREATER_THAN;
        RexNode comparatorNode = rexBuilder.makeCall(call.getType(), operator,
            ImmutableList.of(rexInputRef, rowExpr2.get(i)));
        andNodes.add(comparatorNode);
        // create an AND'ed expression
        RexNode and = RexUtil.composeConjunction(rexBuilder, andNodes);
        orNodes.add(and);
      }
      // create an OR'ed expression
      return RexUtil.composeDisjunction(rexBuilder, orNodes);
    default:
      throw new UnsupportedOperationException(
          "Only greater than operator is supported " + "while comparing two row value constructors");
    }
  }

}
