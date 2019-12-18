package com.twilio.raas.sql.rules;

import com.google.common.collect.Lists;
import com.twilio.raas.sql.KuduQuery;
import com.twilio.raas.sql.KuduRel;
import com.twilio.raas.sql.rel.KuduSortRel;
import com.twilio.raas.sql.rel.KuduToEnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KuduSortAggregationLimitRule extends KuduSortRule {
  public KuduSortAggregationLimitRule(RelBuilderFactory relBuilderFactory) {
    super(operand(EnumerableLimit.class,
        operand(Sort.class,
            some(operand(Aggregate.class,
                some(operand(KuduToEnumerableRel.class,
                    some(operand(Project.class,
                        some(operand(Filter.class,
                            some(operand(KuduQuery.class, none())))))))))))), relBuilderFactory,
        "KuduSortAggregationTransposeRule");
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    final EnumerableLimit originalLimit = (EnumerableLimit) call.getRelList().get(0);
    final Sort originalSort = (Sort) call.getRelList().get(1);
    final Aggregate originalAggregate = (Aggregate) call.getRelList().get(2);
    final KuduToEnumerableRel kuduToEnumerableRel =
        (KuduToEnumerableRel) call.getRelList().get(3);
    final Project project = (Project) call.getRelList().get(4);
    final Filter filter = (Filter) call.getRelList().get(5);
    final KuduQuery query = (KuduQuery) call.getRelList().get(6);

    /**
     * This rule should check that the columns being grouped by are also present in sort.
     *
     * For a table with PK(A,B)
     * A, B, C,
     * 1, 1, 1,
     * 1, 2, 2,
     * 1, 3, 1,
     * A query that does GROUP BY A, C ORDER BY A cannot use this rule.
     */
    for (Integer groupedOrdinal: originalAggregate.getGroupSet()) {
      if (groupedOrdinal < query.openedTable.getSchema().getPrimaryKeyColumnCount()) {
        boolean found = false;
        for (RelFieldCollation fieldCollation : originalSort.getCollation().getFieldCollations()) {
          if (fieldCollation.getFieldIndex() == groupedOrdinal) {
            found = true;
            break;
          }
        }
        if (!found) {
          return;
        }
      } else {
        // group by field is not a member of the primary key. Order cannot be exploited for group keys.
        return;
      }
    }
    // Take the sorted fields and remap them through the group ordinals and then through the
    // project ordinals.
    // This maps directly to the Kudu field indexes. The Map is between originalSort ordinal ->
    // Kudu Field index.
    final Map<Integer, Integer> remappingOrdinals = new HashMap<>();

    final List<Integer> groupSet = originalAggregate.getGroupSet().asList();
    for (RelFieldCollation fieldCollation: originalSort.getCollation().getFieldCollations()) {
      int groupOrdinal = fieldCollation.getFieldIndex();
      int projectOrdinal = groupSet.get(groupOrdinal);
      int kuduColumnIndex = ((RexInputRef)project.getProjects().get(projectOrdinal)).getIndex();
      remappingOrdinals.put(fieldCollation.getFieldIndex(), kuduColumnIndex);
    }

    RelCollation newCollation = RelCollationTraitDef.INSTANCE.canonize(
        RelCollations.permute(originalSort.getCollation(), remappingOrdinals));
    final RelTraitSet traitSet = originalSort.getTraitSet()
        .plus(newCollation)
        .plus(Convention.NONE);

    // Check the new trait set to see if we can apply the sort against this.
    if (!canApply(traitSet, query, query.openedTable, Optional.of(filter))) {
      return;
    }

    final KuduSortRel newSort = new KuduSortRel(
        project.getCluster(),
        traitSet.replace(KuduRel.CONVENTION),
        convert(project, project.getTraitSet().replace(RelCollations.EMPTY)),
        newCollation,
        originalLimit.offset,
        originalLimit.fetch,
        true);

    final RelNode newkuduToEnumerableRel =
        kuduToEnumerableRel.copy(kuduToEnumerableRel.getTraitSet(), Lists.newArrayList(newSort));

    // Create a the aggregation relation and indicate it is sorted based on result.
    final RelNode newAggregation = originalAggregate.copy(
        originalAggregate.getTraitSet()
            .replace(traitSet.getTrait(RelCollationTraitDef.INSTANCE)),
        Collections.singletonList(newkuduToEnumerableRel));

    call.transformTo(newAggregation);
  }
}
