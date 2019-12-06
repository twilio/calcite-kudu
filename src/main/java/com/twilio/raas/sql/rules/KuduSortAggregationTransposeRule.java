package com.twilio.raas.sql.rules;

import java.util.Collections;
import java.util.Optional;

import com.twilio.raas.sql.KuduQuery;
import com.twilio.raas.sql.KuduRel;
import com.twilio.raas.sql.rel.KuduSortRel;
import com.twilio.raas.sql.rel.KuduToEnumerableRel;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
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

    if (!canApply(originalSort, query, query.openedTable, Optional.of(input))) {
      return;
    }

    final RelTraitSet traitSet = originalSort.getTraitSet().replace(KuduRel.CONVENTION);

    final KuduSortRel newSort = new KuduSortRel(
        input.getCluster(),
        traitSet,
        convert(input, traitSet.replace(RelCollations.EMPTY)),
        originalSort.getCollation(),
        originalSort.offset,
        originalSort.fetch,
        true);

    final RelNode newKuduEnumerable = new KuduToEnumerableRel(originalSort.getCluster(),
        originalSort.getTraitSet(), newSort);

    // Create a the aggregation relation and indicate it is sorted based on result.
    final RelNode newAggregation = originalAggregate.copy(
        originalAggregate.getTraitSet()
            .replace(RelCollationTraitDef.INSTANCE.canonize(originalSort.getCollation())),
        Collections.singletonList(newKuduEnumerable));

    call.transformTo(newAggregation);
  }
}
