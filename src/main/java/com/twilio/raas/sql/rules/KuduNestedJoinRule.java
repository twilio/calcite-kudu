package com.twilio.raas.sql.rules;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.twilio.raas.sql.KuduRel;
import com.twilio.raas.sql.rel.KuduNestedJoin;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

public class KuduNestedJoinRule extends RelOptRule {
    public final static EnumSet<SqlKind> VALID_CALL_TYPES = EnumSet.of(SqlKind.EQUALS, SqlKind.GREATER_THAN,
            SqlKind.GREATER_THAN_OR_EQUAL, SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL);

    public static final int DEFAULT_BATCH_SIZE = 100;
    private int batchSize;

    public KuduNestedJoinRule(RelBuilderFactory relBuilderFactory) {
        super(operand(Join.class, operand(KuduRel.class, any())),
            relBuilderFactory, "KuduNestedJoin");
        this.batchSize = DEFAULT_BATCH_SIZE;
    }

    @Override
    public boolean matches(final RelOptRuleCall call) {
        final Join join = call.rel(0);
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
                        if (opResult == null || opResult == Boolean.FALSE) {
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
        final Join join = call.rel(0);

        final Set<CorrelationId> correlationIds = new HashSet<>();

        final ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();

        final JoinRelType joinType = join.getJoinType();

        call.transformTo(KuduNestedJoin.create(
                convert(join.getLeft(), join.getLeft().getTraitSet().replace(EnumerableConvention.INSTANCE)),
                convert(join.getRight(), join.getRight().getTraitSet().replace(EnumerableConvention.INSTANCE)),
                join.getCondition(),
                requiredColumns.build(), correlationIds, joinType));
    }
}
