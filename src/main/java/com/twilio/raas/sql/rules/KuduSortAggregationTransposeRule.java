package com.twilio.raas.sql.rules;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.twilio.raas.sql.KuduQuery;
import com.twilio.raas.sql.KuduRel;
import com.twilio.raas.sql.rel.KuduSortRel;
import com.twilio.raas.sql.rel.KuduToEnumerableRel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.tools.RelBuilderFactory;

public class KuduSortAggregationTransposeRule extends KuduSortRule {
  public KuduSortAggregationTransposeRule(RelBuilderFactory relBuilderFactory) {
    super(operand(Sort.class,
                    some(operand(Aggregate.class,
                            some(operand(KuduToEnumerableRel.class,
                                    some(operand(Filter.class,
                                            some(operand(KuduQuery.class,
                                                    none()))))))))),
        relBuilderFactory,
        "KuduSortAggregationTransposeRule");
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    final Sort originalSort = (Sort) call.getRelList().get(0);
    final Aggregate originalAggregate = (Aggregate) call.getRelList().get(1);
    // Rel(2) is the KuduToEnumerableRel. This is not needed.
    final Filter input = (Filter) call.getRelList().get(3);
    final KuduQuery query = (KuduQuery) call.getRelList().get(4);

    /**
     * This rule should checks that the columns being grouped by are also present in sort.
     *
     * For table with PK(A,B)
     * A, B, C,
     * 1, 1, 1,
     * 1, 2, 2,
     * 1, 3, 1,
     * For a query that does group by A, C and an order by A the rule cannot apply the sort.
     */
    // Take the sorted fields and remap them through the group ordinals. This maps directly to the
    // Kudu field indexes. The Map is between originalSort ordinal -> Kudu Field index.
    final Map<Integer, Integer> remappingOrdinals = new HashMap<>();

    final List<Integer> groupSet = originalAggregate.getGroupSet().asList();
    for (RelFieldCollation fieldCollation: originalSort.getCollation().getFieldCollations()) {
      remappingOrdinals.put(fieldCollation.getFieldIndex(),
          groupSet.get(fieldCollation.getFieldIndex()));
    }

    RelCollation newCollation = RelCollationTraitDef.INSTANCE.canonize(
        RelCollations.permute(originalSort.getCollation(), remappingOrdinals));
    final RelTraitSet traitSet = originalSort.getTraitSet()
      .plus(newCollation)
      .plus(Convention.NONE);

    // Check the new trait set to see if we can apply the sort against this.
    if (!canApply(traitSet, query, query.openedTable, Optional.of(input))) {
      return;
    }

    final KuduSortRel newSort = new KuduSortRel(
        input.getCluster(),
        traitSet.replace(KuduRel.CONVENTION),
        convert(input, input.getTraitSet().replace(RelCollations.EMPTY)),
        newCollation,
        originalSort.offset,
        originalSort.fetch,
        true);

    final RelNode newKuduEnumerable = new KuduToEnumerableRel(originalSort.getCluster(),
        traitSet, newSort);

    // Create a the aggregation relation and indicate it is sorted based on result.
    final RelNode newAggregation = originalAggregate.copy(
        originalAggregate.getTraitSet()
        .replace(traitSet.getTrait(RelCollationTraitDef.INSTANCE)),
        Collections.singletonList(newKuduEnumerable));

    call.transformTo(newAggregation);
  }
}
