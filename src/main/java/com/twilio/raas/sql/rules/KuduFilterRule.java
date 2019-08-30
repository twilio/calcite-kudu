package com.twilio.raas.sql.rules;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.RangeSet;
import com.twilio.raas.sql.CalciteKuduPredicate;
import com.twilio.raas.sql.KuduQuery;
import com.twilio.raas.sql.KuduRel;
import com.twilio.raas.sql.rel.KuduFilterRel;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KuduFilterRule extends RelOptRule {
    public KuduFilterRule(RelBuilderFactory relBuilderFactory) {
        super(operand(LogicalFilter.class, operand(KuduQuery.class, none())),
              relBuilderFactory, "KuduPushDownFilters");
    }

    public static RexNode transformRowValueExpressions(RexBuilder rexBuilder, RexNode node) {
        return node.accept(new RowValueExpressionConverter(rexBuilder));
    }

    /**
     * When we match, this method needs to transform the
     * {@link RelOptRuleCall} into a new rule with push
     * filters applied.
     */
    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = (LogicalFilter)call.getRelList().get(0);
        final KuduQuery scan = (KuduQuery)call.getRelList().get(1);
        if (filter.getTraitSet().contains(Convention.NONE)) {
            final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
            // expand row value expression into a series of OR-AND expressions
            final RexNode condition = transformRowValueExpressions(rexBuilder, filter.getCondition());
            final KuduPredicatePushDownVisitor predicateParser = new KuduPredicatePushDownVisitor(scan.openedTable.getSchema());
            List<List<CalciteKuduPredicate>> predicates = condition.accept(predicateParser, null);
            if (predicates.isEmpty()) {
                // if we could not handle all the filters in Kudu, just return and let Calcite
                // handle filtering
                return;
            }
            final RelNode converted = new KuduFilterRel(filter.getCluster(),
                                                        filter.getTraitSet().replace(KuduRel.CONVENTION),
                                                        convert(filter.getInput(), KuduRel.CONVENTION), // @TODO: what is this call
                                                        filter.getCondition(),
                                                        predicates);

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
