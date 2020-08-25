package com.twilio.raas.sql.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.twilio.raas.sql.CalciteKuduPredicate;
import com.twilio.raas.sql.ComparisonPredicate;
import com.twilio.raas.sql.InListPredicate;
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
import org.apache.kudu.client.KuduPredicate;

public class KuduFilterRule extends RelOptRule {
    public KuduFilterRule(RelBuilderFactory relBuilderFactory) {
        super(operand(LogicalFilter.class, operand(KuduQuery.class, none())),
              relBuilderFactory, "KuduPushDownFilters");
    }

    /**
     * When we match, this method needs to transform the {@link RelOptRuleCall} into
     * a new rule with push filters applied.
     */
    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = (LogicalFilter) call.getRelList().get(0);
        final KuduQuery kuduQuery = (KuduQuery) call.getRelList().get(1);
        if (filter.getTraitSet().contains(Convention.NONE)) {
            final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
            // expand row value expression into a series of OR-AND expressions
            RowValueExpressionConverter visitor = new RowValueExpressionConverter(rexBuilder,
                    kuduQuery.calciteKuduTable);
            final RexNode condition = filter.getCondition().accept(visitor);
            final KuduPredicatePushDownVisitor predicateParser = new KuduPredicatePushDownVisitor();

            // Parse condition for filters to push down to Kudu and then look at each kudu
            // scan to find common fields with EQUAL conditions to join together.
            List<List<CalciteKuduPredicate>> predicates = processForInList(
                condition.accept(predicateParser, null));
            if (predicates.isEmpty()) {
                // if we could not handle any of the filters in Kudu, just return and let Calcite
                // handle filtering
                return;
            }
            final RelNode converted = new KuduFilterRel(
                filter.getCluster(),
                filter.getTraitSet().replace(KuduRelNode.CONVENTION),
                convert(filter.getInput(), KuduRelNode.CONVENTION),
                filter.getCondition(),
                predicates,
                kuduQuery.calciteKuduTable.getKuduTable().getSchema(),
                !predicateParser.areAllFiltersApplied());

            call.transformTo(converted);
        }
    }

    static Set<Integer> columnsInAllScans(
        final List<List<CalciteKuduPredicate>> scans, final Set<Integer> allFields) {

        return allFields.stream()
                .filter(i -> scans.stream().allMatch(
                        subScan -> subScan.stream().filter(p -> p.inListOptimizationAllowed(i))
                        // Each scan must have this predicate exactly once.
                        // https://twilioincidents.appspot.com/incident/4859603272597504/view
                        .count() == 1L))
                .collect(Collectors.toSet());
    }

    static List<List<CalciteKuduPredicate>> processForInList(
        final List<List<CalciteKuduPredicate>> original) {

        // Collect all column indexes in all scans
        final HashSet<Integer> allFields = new HashSet<>();
        for (List<CalciteKuduPredicate> scan : original) {
            for (CalciteKuduPredicate pred : scan) {
                allFields.add(pred.getColumnIdx());
            }
        }

        // Find column indices that are shared in each scan and each scan uses a EQUAL
        // on that field.
        final Set<Integer> inAllScans = columnsInAllScans(original, allFields);
        // When there isn't any fields with in list optimization, return original.
        if (inAllScans.isEmpty()) {
            return original;
        }

        // Initialize a Map from Column Index to Set of all values. A Set is used so
        // remove duplicate scan predicates -- for instance account_sid = AC123 being present
        // in each scan.
        final Map<Integer, HashSet<Object>> inPredicates = inAllScans.stream()
                .collect(Collectors.<Integer, Integer, HashSet<Object>>toMap(
                             c -> c, c -> new HashSet<Object>()));

        Set<List<CalciteKuduPredicate>> updatedPredicates = new HashSet<>();
        for (List<CalciteKuduPredicate> scan : original) {
            final ArrayList<CalciteKuduPredicate> updatedScan = new ArrayList<>();

            for (CalciteKuduPredicate pred : scan) {
                if (!inAllScans.contains(pred.getColumnIdx())) {
                    updatedScan.add(pred);
                } else {
                    inPredicates.get(pred.getColumnIdx())
                        .add(((ComparisonPredicate) pred)
                             .rightHandValue);
                }
            }
            if (!updatedScan.isEmpty()) {
                updatedPredicates.add(updatedScan);
            }
        }

        int inListCount = 0;
        final List<CalciteKuduPredicate> inListScan = new ArrayList<>();
        for (Map.Entry<Integer, HashSet<Object>> idxListPair : inPredicates.entrySet()) {
            // If there is only one value, use EQUAL instead of IN LIST.
            if (idxListPair.getValue().size() == 1) {
                inListScan.add(new ComparisonPredicate(idxListPair.getKey(),
                                                       KuduPredicate.ComparisonOp.EQUAL,
                                                       idxListPair.getValue().iterator().next()));
            } else {
                inListCount++;
                inListScan.add(new InListPredicate(idxListPair.getKey(),
                                                   new ArrayList<>(idxListPair.getValue())));
            }
        }

        // Optimization created 0 InListPredicates, therefore just return original.
        if (inListCount == 0) {
            return original;
        }

        if (updatedPredicates.isEmpty()) {
            return Collections.singletonList(inListScan);
        } else if (updatedPredicates.size() == 1) {
          // Similar to {@link KuduPredicatePushDownVisitor#mergePredicateLists(AND, left, right)}
            updatedPredicates.stream().forEach(scan -> scan.addAll(inListScan));
            return new ArrayList<>(updatedPredicates);
        } else {
          return original;
        }

    }
}
