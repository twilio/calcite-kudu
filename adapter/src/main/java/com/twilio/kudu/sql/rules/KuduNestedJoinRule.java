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

import com.twilio.kudu.sql.rel.KuduNestedJoin;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class KuduNestedJoinRule extends RelOptRule {
  public final static EnumSet<SqlKind> VALID_CALL_TYPES = EnumSet.of(SqlKind.EQUALS, SqlKind.GREATER_THAN,
      SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL);

  public static final int DEFAULT_BATCH_SIZE = 5;
  private int batchSize;

  public static final String HINT_NAME = "USE_KUDU_NESTED_JOIN";

  public KuduNestedJoinRule(RelBuilderFactory relBuilderFactory) {
    this(relBuilderFactory, DEFAULT_BATCH_SIZE);
  }

  public KuduNestedJoinRule(RelBuilderFactory relBuilderFactory, final int batchSize) {
    super(operand(LogicalJoin.class, any()), relBuilderFactory, "KuduNestedJoinRule");
    this.batchSize = DEFAULT_BATCH_SIZE;
  }

  @Override
  public boolean matches(final RelOptRuleCall call) {
    final Join join = call.rel(0);
    // only match this rule if a hint is specified
    if (!join.getHints().stream().map(h -> h.hintName).anyMatch( s -> s.equalsIgnoreCase(HINT_NAME))
      || (join.getJoinType() != JoinRelType.INNER && join.getJoinType() != JoinRelType.LEFT)) {
      return false;
    }

    final RexNode condition = join.getCondition();
    final RexVisitor<Boolean> validateJoinCondition = new RexVisitorImpl<Boolean>(true) {
      @Override
      public Boolean visitCall(final RexCall rexCall) {
        final SqlKind callType = rexCall.getOperator().getKind();
        if (callType == SqlKind.OR) {
          return Boolean.FALSE;
        } else if (callType == SqlKind.AND) {
          for (final RexNode operand : rexCall.operands) {
            final Boolean opResult = operand.accept(this);
            if (opResult == null || opResult.equals(Boolean.FALSE)) {
              return Boolean.FALSE;
            }
          }
        } else if (!VALID_CALL_TYPES.contains(callType)) {
          return Boolean.FALSE;
        }
        return Boolean.TRUE;
      }

      @Override
      public Boolean visitInputRef(final RexInputRef inputRef) {
        return Boolean.TRUE;
      }
    };

    return condition.accept(validateJoinCondition);
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    LogicalJoin join = call.rel(0);
    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : join.getInputs()) {
      if (!(input.getConvention() instanceof EnumerableConvention)) {
        input =
          convert(
            input,
            input.getTraitSet()
              .replace(EnumerableConvention.INSTANCE));
      }
      newInputs.add(input);
    }
    final RelNode left = newInputs.get(0);
    final RelNode right = newInputs.get(1);

    final JoinRelType joinType = join.getJoinType();

    final KuduNestedJoin newJoin = KuduNestedJoin.create(left, right,
      join.getCondition(), joinType, this.batchSize);

    call.transformTo(newJoin);
  }

}
