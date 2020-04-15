package com.twilio.raas.sql.rules;

import java.util.Optional;

import com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Similar to {@link org.apache.calcite.rel.rules.SortJoinTransposeRule} rule except it can
 * handle a filter in between the sort and join
 */
public abstract class KuduSortJoinTransposeRule extends RelOptRule {

    public KuduSortJoinTransposeRule(RelOptRuleOperand operand,
        RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    protected boolean doesMatch(final Sort sort, final Join join, final RelMetadataQuery mq) {
        final JoinInfo joinInfo = JoinInfo.of(
                join.getLeft(), join.getRight(), join.getCondition());

        // 1) If join is not a left or right outer, we bail out
        // 2) If sort is not a trivial order-by, and if there is
        // any sort column that is not part of the input where the
        // sort is pushed, we bail out
        // 3) If sort has an offset, and if the non-preserved side
        // of the join is not count-preserving against the join
        // condition, we bail out
        if (!join.getJoinType().generatesNullsOnLeft()) {
            if (sort.getCollation() != RelCollations.EMPTY) {
                for (RelFieldCollation relFieldCollation
                        : sort.getCollation().getFieldCollations()) {
                    if (relFieldCollation.getFieldIndex()
                            >= join.getLeft().getRowType().getFieldCount()) {
                        return false;
                    }
                }
            }
            if (sort.offset != null
                    && !RelMdUtil.areColumnsDefinitelyUnique(
                    mq, join.getRight(), joinInfo.rightSet())) {
                return false;
            }
        }
        else if (!join.getJoinType().generatesNullsOnRight()) {
            if (sort.getCollation() != RelCollations.EMPTY) {
                for (RelFieldCollation relFieldCollation
                        : sort.getCollation().getFieldCollations()) {
                    if (relFieldCollation.getFieldIndex()
                            < join.getLeft().getRowType().getFieldCount()) {
                        return false;
                    }
                }
            }
            if (sort.offset != null
                    && !RelMdUtil.areColumnsDefinitelyUnique(
                    mq, join.getLeft(), joinInfo.leftSet())) {
                return false;
            }
        }
        else {
            return false;
        }

        return sort.getCollation() != RelCollations.EMPTY;
    }

    protected void perform(final RelOptRuleCall call,  final Sort sort, final Join join, final Optional<Filter> filter) {
        // We create a new sort operator on the corresponding input
        final RelNode newLeftInput;
        final RelNode newRightInput;
        final RelMetadataQuery mq = call.getMetadataQuery();
        if (!join.getJoinType().generatesNullsOnLeft()) {
            // If the[] input is already sorted and we are not reducing the number of tuples,
            // we bail out
            if (RelMdUtil.checkInputForCollationAndLimit(mq, join.getLeft(),
                    sort.getCollation(), sort.offset, sort.fetch)) {
                return;
            }
            // Because it is inner we cannot push the fetch and  offset down. We do no know how many
            // rows will need to be fetched on the left or right side.
            if (join.getJoinType() == JoinRelType.INNER && (sort.fetch != null || sort.offset != null)) {
                newLeftInput = sort.copy(sort.getTraitSet(), join.getLeft(), sort.getCollation(),
                    sort.offset, sort.fetch);
            }
            else {
                newLeftInput = sort.copy(sort.getTraitSet(), join.getLeft(), sort.getCollation(),
                    sort.offset, sort.fetch);
            }
            newRightInput = join.getRight();
        }
        else {
            final RelCollation rightCollation =
                    RelCollationTraitDef.INSTANCE.canonize(
                            RelCollations.shift(sort.getCollation(),
                                    -join.getLeft().getRowType().getFieldCount()));
            // If the input is already sorted and we are not reducing the number of tuples,
            // we bail out
            if (RelMdUtil.checkInputForCollationAndLimit(mq, join.getRight(),
                    rightCollation, sort.offset, sort.fetch)) {
                return;
            }
            newLeftInput = join.getLeft();
            // Because it is inner we cannot push the fetch and  offset down. We do no know how many
            // rows will need to be fetched on the left or right side.
            if (join.getJoinType() == JoinRelType.INNER && (sort.fetch != null || sort.offset != null)) {
                newRightInput = sort.copy(sort.getTraitSet().replace(rightCollation),
                    join.getRight(), rightCollation, null, null);
            }
            else {
                newRightInput = sort.copy(sort.getTraitSet().replace(rightCollation),
                    join.getRight(), rightCollation, sort.offset, sort.fetch);
            }
        }

        final RelNode joinCopy = join.copy(join.getTraitSet(), join.getCondition(), newLeftInput,
                newRightInput, join.getJoinType(), join.isSemiJoinDone());
        // since the filter wraps the sort operator create a copy of the filter with the sort's
        // trait so that the sort can get optimized out
        final RelNode transformTarget;
        if (filter.isPresent()) {
            transformTarget = filter.get().copy(filter.get().getTraitSet(), Lists.newArrayList(joinCopy));
        }
        else {
            transformTarget = joinCopy;
        }

        // Because it is inner we cannot push the fetch and  offset down. We do no know how many
        // rows will need to be fetched on the left or right side.
        if (join.getJoinType() == JoinRelType.INNER && (sort.fetch != null || sort.offset != null)) {
            final Sort limitNode = LogicalSort.create(transformTarget, RelCollations.EMPTY, sort.offset, sort.fetch);
            call.transformTo(limitNode);
        }
        else {
            call.transformTo(transformTarget);
        }
    }

    public static class KuduSortAboveFilter extends KuduSortJoinTransposeRule {
        public KuduSortAboveFilter(final RelBuilderFactory relBuilderFactory) {
            super(
                operand(
                    LogicalSort.class,
                    operand(LogicalFilter.class,
                        operand(LogicalJoin.class,
                            any()))),
                relBuilderFactory, null);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final Sort sort = call.rel(0);
            final Join join = call.rel(2);
            return doesMatch(sort, join, call.getMetadataQuery());
        }
        @Override
        public void onMatch(RelOptRuleCall call) {
            final Sort sort = call.rel(0);
            final Filter filter = call.rel(1);
            final Join join = call.rel(2);

            perform(call, sort, join, Optional.of(filter));
        }
    }


    public static class KuduSortAboveJoin extends KuduSortJoinTransposeRule {
        public KuduSortAboveJoin(final RelBuilderFactory relBuilderFactory) {
            super(operand(LogicalSort.class,
                    operand(LogicalJoin.class, any())),
                relBuilderFactory, null);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final Sort sort = call.rel(0);
            final Join join = call.rel(1);
            return doesMatch(sort, join, call.getMetadataQuery());
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final Sort sort = call.rel(0);
            final Join join = call.rel(1);

            perform(call, sort, join, Optional.empty());
        }
    }
}
