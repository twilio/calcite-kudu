package com.twilio.raas.sql.rules;

import com.google.common.collect.ImmutableList;
import com.twilio.raas.sql.CalciteKuduPredicate;
import com.twilio.raas.sql.KuduQuery;
import com.twilio.raas.sql.KuduRelNode;
import com.twilio.raas.sql.rel.KuduFilterRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class KuduFilterRule extends RelOptRule {
    public KuduFilterRule(RelBuilderFactory relBuilderFactory) {
        super(operand(LogicalFilter.class, operand(KuduQuery.class, none())),
              relBuilderFactory, "KuduPushDownFilters");
    }

  /**
     * When we match, this method needs to transform the
     * {@link RelOptRuleCall} into a new rule with push
     * filters applied.
     */
    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = (LogicalFilter)call.getRelList().get(0);
        final KuduQuery kuduQuery = (KuduQuery)call.getRelList().get(1);
        if (filter.getTraitSet().contains(Convention.NONE)) {
            final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
            // expand row value expression into a series of OR-AND expressions
          RowValueExpressionConverter visitor = new RowValueExpressionConverter(rexBuilder,
            kuduQuery.calciteKuduTable);
          final RexNode condition = filter.getCondition().accept(visitor);
            final KuduPredicatePushDownVisitor predicateParser = new KuduPredicatePushDownVisitor();
            List<List<CalciteKuduPredicate>> predicates = condition.accept(predicateParser, null);
            if (predicates.isEmpty()) {
                // if we could not handle all the filters in Kudu, just return and let Calcite
                // handle filtering
                return;
            }
            final RelNode converted = new KuduFilterRel(filter.getCluster(),
              filter.getTraitSet().replace(KuduRelNode.CONVENTION), convert(filter.getInput(),
              KuduRelNode.CONVENTION), filter.getCondition(), predicates,
              kuduQuery.calciteKuduTable.getKuduTable().getSchema());

            if (predicateParser.areAllFiltersApplied()) {
                // if we can push down all filters to kudu then we don't need to filter rows in calcite
                call.transformTo(converted);
            }
            else {
                // push down filters that kudu can handle and let calcite filter as well so that
                // all filters are evaluated
                // TODO see if we can remove filters that are handled by kudu so that those filters
                // are not evaluated a second time in calcite
                call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(converted)));
            }
        }
    }
}
