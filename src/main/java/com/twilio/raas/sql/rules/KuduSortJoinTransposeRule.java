package com.twilio.raas.sql.rules;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
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
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Similar to {@link org.apache.calcite.rel.rules.SortJoinTransposeRule} rule except it can
 * handle a filter in between the sort and join
 */
public class KuduSortJoinTransposeRule extends RelOptRule {

    public KuduSortJoinTransposeRule(Class<? extends Sort> sortClass,
                                     Class<? extends Filter> filterClass,
                                 Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
        super(
                operand(sortClass,
                        operand(filterClass,
                                operand(joinClass, any()))),
                relBuilderFactory, null);
    }

    @Override public boolean matches(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final Join join = call.rel(2);
        final RelMetadataQuery mq = call.getMetadataQuery();
        final JoinInfo joinInfo = JoinInfo.of(
                join.getLeft(), join.getRight(), join.getCondition());

        // 1) If join is not a left or right outer, we bail out
        // 2) If sort is not a trivial order-by, and if there is
        // any sort column that is not part of the input where the
        // sort is pushed, we bail out
        // 3) If sort has an offset, and if the non-preserved side
        // of the join is not count-preserving against the join
        // condition, we bail out
        if (join.getJoinType() == JoinRelType.LEFT) {
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
        } else if (join.getJoinType() == JoinRelType.RIGHT) {
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
        } else {
            return false;
        }

        return true;
    }

    @Override public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        final Filter filter = call.rel(1);
        final Join join = call.rel(2);

        // We create a new sort operator on the corresponding input
        final RelNode newLeftInput;
        final RelNode newRightInput;
        final RelMetadataQuery mq = call.getMetadataQuery();
        if (join.getJoinType() == JoinRelType.LEFT) {
            // If the input is already sorted and we are not reducing the number of tuples,
            // we bail out
            if (RelMdUtil.checkInputForCollationAndLimit(mq, join.getLeft(),
                    sort.getCollation(), sort.offset, sort.fetch)) {
                return;
            }
            newLeftInput = sort.copy(sort.getTraitSet(), join.getLeft(), sort.getCollation(),
                    sort.offset, sort.fetch);
            newRightInput = join.getRight();
        } else {
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
            newRightInput = sort.copy(sort.getTraitSet().replace(rightCollation),
                    join.getRight(), rightCollation, sort.offset, sort.fetch);
        }

        final RelNode joinCopy = join.copy(join.getTraitSet(), join.getCondition(), newLeftInput,
                newRightInput, join.getJoinType(), join.isSemiJoinDone());
        // since the filter wraps the sort operator create a copy of the filter with the sort's
        // trait so that the sort can get optimized out
        final RelNode filterCopy = filter.copy(sort.getTraitSet(), Lists.newArrayList(joinCopy));
        final RelNode sortCopy = sort.copy(sort.getTraitSet(), filterCopy, sort.getCollation(),
                sort.offset, sort.fetch);

        call.transformTo(sortCopy);
    }

}