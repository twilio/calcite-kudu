package com.twilio.raas.sql.rules;

import java.util.Collections;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.tools.RelBuilderFactory;

public final class SortInnerJoinTranspose extends RelOptRule {
    public SortInnerJoinTranspose(final RelBuilderFactory factory) {
        super(operand(LogicalSort.class,
                operand(LogicalJoin.class, any())),
                factory, "SortInnerJoinTranspose");
    }

    @Override
    public boolean matches(final RelOptRuleCall call) {
        final LogicalSort sort = (LogicalSort) call.getRelList().get(0);
        final LogicalJoin join = (LogicalJoin) call.getRelList().get(1);

        final JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());

        final RelMetadataQuery mq = call.getMetadataQuery();

        if (sort.offset != null
            && !RelMdUtil.areColumnsDefinitelyUnique(mq, join.getRight(), joinInfo.rightSet())) {
            return false;
        }

        if (sort.offset != null && !RelMdUtil.areColumnsDefinitelyUnique(mq, join.getLeft(), joinInfo.leftSet())) {
            return false;
        }

        return join.getJoinType() == JoinRelType.INNER &&
            sort.getCollation() != RelCollations.EMPTY;
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
        final LogicalSort sort = (LogicalSort) call.getRelList().get(0);
        final LogicalJoin join = (LogicalJoin) call.getRelList().get(1);
        final RelMetadataQuery mq = call.getMetadataQuery();

        if (join.getJoinType() != JoinRelType.INNER ||
            sort.getCollation() == RelCollations.EMPTY) {
            return;
        }

        // look at the collation to see if all the fields are on the left.
        boolean sortOnLeft = true;
        boolean sortOnRight = true;
        for (RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations()) {
            if (relFieldCollation.getFieldIndex() >= join.getLeft().getRowType().getFieldCount()) {
                sortOnLeft = false;
            }

            if (relFieldCollation.getFieldIndex() < join.getLeft().getRowType().getFieldCount()) {
                sortOnRight = false;
            }
        }
        final RelNode newLeftInput;
        final RelNode newRightInput;

        if (sortOnLeft && sortOnRight) {
            throw new IllegalArgumentException("Logic to push sort through inner join did not work");
        }
        if (sortOnLeft) {
            if (RelMdUtil.checkInputForCollationAndLimit(mq, join.getLeft(), sort.getCollation(), sort.offset,
                    sort.fetch)) {
                return;
            }
            // Remove the fetch and offset
            newLeftInput = sort.copy(sort.getTraitSet(), join.getLeft(), sort.getCollation(), null, null);
            // @TODO: should we be copying instead of by reference?
            newRightInput = join.getRight();
        }
        else if (sortOnRight) {
            final RelCollation rightCollation = RelCollationTraitDef.INSTANCE
                .canonize(RelCollations.shift(sort.getCollation(), -join.getLeft().getRowType().getFieldCount()));
            if (RelMdUtil.checkInputForCollationAndLimit(mq, join.getRight(),
                    rightCollation, sort.offset, sort.fetch)) {
                return;
            }
            newLeftInput = join.getLeft();
            newRightInput = sort.copy(sort.getTraitSet().replace(rightCollation), join.getRight(), rightCollation, null,
                    null);

        }
        else {
            // Can't push the sort on either side.
            return;
        }

        final RelNode joinCopy = join.copy(join.getTraitSet(), join.getCondition(), newLeftInput,
            newRightInput, join.getJoinType(), join.isSemiJoinDone());

        if (sort.fetch != null || sort.offset != null) {
            // Because INNER join is just like a FILTER, we cannot enforce a limit. This pushes the
            // limit on top.
            final RelNode limit = LogicalSort.create(joinCopy, sort.getCollation(), sort.offset, sort.fetch);

            call.transformTo(limit);
        }
        else {
            call.transformTo(joinCopy);
        }
    }
}
