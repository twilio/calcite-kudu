package com.twilio.raas.sql.rules;

import com.twilio.raas.sql.KuduQuery;
import com.twilio.raas.sql.KuduRel;
import com.twilio.raas.sql.rel.KuduFilterRel;
import com.twilio.raas.sql.rel.KuduSortRel;
import com.twilio.raas.sql.rel.KuduToEnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.kudu.client.KuduTable;

/**
 * When a sort includes one or more of the primary keys  in the sorted
 * direction but the other primary key is in an equivalence filter, our
 * Enumerable can still return the result that respects the sort.
 *
 * For instance, a query that filters on account_id -- which is the first
 * key -- and sorts by date_created field which is the second (and a range)
 * key, the enumerable can return the results in sorted order.
 */
public class KuduSortWithFilterRule extends RelOptRule {

    public static final RelOptRuleOperand OPERAND =
            operand(KuduToEnumerableRel.class, some(
                    operand(KuduFilterRel.class, some(operand(KuduQuery.class, none())))));

    public KuduSortWithFilterRule(RelBuilderFactory relBuilderFactory) {
        super(operand(Sort.class, OPERAND),
                relBuilderFactory, "KuduFilterSort");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final KuduQuery query = (KuduQuery)call.getRelList().get(3);
        final KuduFilterRel filter = (KuduFilterRel)call.getRelList().get(2);
        final KuduTable openedTable = query.openedTable;
        final Sort originalSort = (Sort)call.getRelList().get(0);
        int mustMatch = 0;
        RexNode originalCondition = filter.getCondition();

        // If there is no sort don't pay the cost of returning rows in sorted order.
        if (originalSort.getCollation().getFieldCollations().isEmpty()) {
            return;
        }

        for (RelFieldCollation sortField: originalSort.getCollation().getFieldCollations()) {
            // Reject for descending sorted fields if sort direction is not Descending
            if ((query.descendingSortedFieldIndices.contains(sortField.getFieldIndex()) &&
                sortField.direction != RelFieldCollation.Direction.DESCENDING &&
                sortField.direction != RelFieldCollation.Direction.STRICTLY_DESCENDING) ||
                // Else Reject if sort order is not ascending
                (!query.descendingSortedFieldIndices.contains(sortField.getFieldIndex()) &&
                    sortField.direction != RelFieldCollation.Direction.ASCENDING &&
                    sortField.direction != RelFieldCollation.Direction.STRICTLY_ASCENDING))
            {
                return;
            }
            if (sortField.getFieldIndex() >= openedTable.getSchema().getPrimaryKeyColumnCount()) {
                // This field is not in the primary key columns. Can't sort this.
                return;
            }
            if (sortField.getFieldIndex() != mustMatch) {
                // Go look at the condition to see if we have an exact match on the condition.
                while (mustMatch < sortField.getFieldIndex()) {
                    final KuduFilterVisitor visitor = new KuduFilterVisitor(mustMatch);
                    final Boolean foundFieldInCondition = originalCondition.accept(visitor);
                    if (foundFieldInCondition == Boolean.FALSE) {
                        return;
                    }
                    mustMatch++;
                }
            }
            mustMatch++;
        }

        // Now transform call into our new rule. This new rule will generate
        // EnumerableRel.Result -- which contains the java code to compile.
        final RelNode input = originalSort.getInput();
        final RelTraitSet traitSet =
                originalSort.getTraitSet().replace(KuduRel.CONVENTION)
                        .replace(originalSort.getCollation());
        final RelNode newNode = new KuduSortRel(
                input.getCluster(),
                traitSet,
                convert(input, traitSet.replace(RelCollations.EMPTY)),
            originalSort.getCollation(),
            originalSort.offset,
            originalSort.fetch);
        call.transformTo(newNode);
    }

    public static class KuduFilterVisitor extends RexVisitorImpl<Boolean> {
        public final int mustHave;
        public KuduFilterVisitor(int mustHave) {
            super(true);
            this.mustHave = mustHave;
        }

        public Boolean visitInputRef(RexInputRef inputRef) {
            return inputRef.getIndex() == this.mustHave;
        }

        public Boolean visitLocalRef(RexLocalRef localRef) {
            return Boolean.FALSE;
        }

        public Boolean visitLiteral(RexLiteral literal) {
            return Boolean.FALSE;
        }

        public Boolean visitCall(RexCall call) {
            switch (call.getOperator().getKind()) {
                case EQUALS:
                    return call.operands.get(0).accept(this);
                case AND:
                    for (RexNode operand : call.operands) {
                        if (operand.accept(this) == Boolean.TRUE) {
                            return Boolean.TRUE;
                        }
                    }
                    return Boolean.FALSE;
                case OR:
                    // @TODO: figure this one out. It is very tricky, if each
                    // operand has the exact same value for mustHave then
                    // this should match.
            }
            return Boolean.FALSE;
        }
    }
}
