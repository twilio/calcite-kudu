package com.twilio.raas.sql.rules;

import com.twilio.raas.sql.KuduQuery;
import com.twilio.raas.sql.KuduRel;
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
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.kudu.client.KuduTable;

/**
 * When a query is executed without a filter, check to see if the sort is in
 * Kudu Primary Key order. If so, tell Enumerable to sort results
 */
public class KuduSortWithoutFilterRule extends RelOptRule {

    public static final RelOptRuleOperand OPERAND =
            operand(KuduToEnumerableRel.class,
                    some(operand(KuduQuery.class, none())));

    public KuduSortWithoutFilterRule(RelBuilderFactory relBuilderFactory) {
        super(operand(Sort.class, OPERAND), relBuilderFactory, "KuduSimpleSort");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final KuduQuery query = (KuduQuery)call.getRelList().get(2);
        final KuduTable openedTable = query.openedTable;
        final Sort originalSort = (Sort)call.getRelList().get(0);

        // If there is no sort -- i.e. there is only a limit
        // don't pay the cost of returning rows in sorted order.
        if (originalSort.getCollation().getFieldCollations().isEmpty()) {
            return;
        }

        int mustMatch = 0;

        for (RelFieldCollation sortField: originalSort.getCollation().getFieldCollations()) {
            if (sortField.direction != RelFieldCollation.Direction.ASCENDING &&
                sortField.direction != RelFieldCollation.Direction.STRICTLY_ASCENDING &&
                !query.descendingSortedDateTimeFieldIndices.contains(sortField.getFieldIndex())) {
                return;
            }
            if (sortField.getFieldIndex() >= openedTable.getSchema().getPrimaryKeyColumnCount() ||
                    sortField.getFieldIndex() != mustMatch) {
                // This field is not in the primary key columns. Can't sort this.
                return;
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
}
