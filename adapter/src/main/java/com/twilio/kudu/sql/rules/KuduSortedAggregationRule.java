/* Copyright 2020 Twilio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twilio.kudu.sql.rules;

import com.google.common.collect.Lists;
import com.twilio.kudu.sql.KuduQuery;
import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.rel.KuduSortRel;
import com.twilio.kudu.sql.rel.KuduToEnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Rule that matches a sort over an aggregation with both sort and aggregation
 * using the same columns. The columns must be a prefix of the primary key of
 * the table. This rule eliminates the need for sorting on the client.
 */
public class KuduSortedAggregationRule extends KuduSortRule {

  private static final RelOptRuleOperand OPERAND = operand(Sort.class,
      some(operand(Aggregate.class, some(operand(KuduToEnumerableRel.class,
          some(operand(Project.class, some(operand(Filter.class, some(operand(KuduQuery.class, none())))))))))));

  public static final RelOptRule SORTED_AGGREGATION_RULE = new KuduSortedAggregationRule(RelFactories.LOGICAL_BUILDER);

  public KuduSortedAggregationRule(RelBuilderFactory relBuilderFactory) {
    super(OPERAND, relBuilderFactory, "KuduSortedAggregationRule");
  }

  protected void perform(final RelOptRuleCall call, final Sort originalSort, final Aggregate originalAggregate,
      final KuduToEnumerableRel kuduToEnumerableRel, final Project project, final Filter filter,
      final KuduQuery query) {
    /**
     * This rule should check that the columns being grouped by are also present in
     * sort.
     *
     * For a table with three columns (A, B, C) with PK(A,B) and rows (1, 1, 1), (1,
     * 2, 2), (1, 3, 1)
     *
     * A query that does GROUP BY A, C ORDER BY A cannot use this rule.
     */
    for (Integer groupedOrdinal : originalAggregate.getGroupSet()) {
      if (groupedOrdinal < query.calciteKuduTable.getKuduTable().getSchema().getPrimaryKeyColumnCount()) {
        boolean found = false;
        for (RelFieldCollation fieldCollation : originalSort.getCollation().getFieldCollations()) {
          if (fieldCollation.getFieldIndex() == groupedOrdinal
              && project.getProjects().get(fieldCollation.getFieldIndex()).getKind() == SqlKind.INPUT_REF) {
            found = true;
            break;
          }
        }
        if (!found) {
          return;
        }
      } else {
        // group by field is not a member of the primary key. Order cannot be exploited
        // for group keys.
        return;
      }
    }
    // Take the sorted fields and remap them through the group ordinals and then
    // through the
    // project ordinals.
    // This maps directly to the Kudu field indexes. The Map is between originalSort
    // ordinal ->
    // Kudu Field index.
    final Map<Integer, Integer> remappingOrdinals = new HashMap<>();

    final List<Integer> groupSet = originalAggregate.getGroupSet().asList();
    for (RelFieldCollation fieldCollation : originalSort.getCollation().getFieldCollations()) {
      int groupOrdinal = fieldCollation.getFieldIndex();
      int projectOrdinal = groupSet.get(groupOrdinal);
      int kuduColumnIndex = ((RexInputRef) project.getProjects().get(projectOrdinal)).getIndex();
      remappingOrdinals.put(fieldCollation.getFieldIndex(), kuduColumnIndex);
    }

    RelCollation newCollation = RelCollationTraitDef.INSTANCE
        .canonize(RelCollations.permute(originalSort.getCollation(), remappingOrdinals));
    final RelTraitSet traitSet = originalSort.getTraitSet().plus(newCollation).plus(Convention.NONE);

    // Check the new trait set to see if we can apply the sort against this.
    if (!canApply(traitSet, query, query.calciteKuduTable.getKuduTable(), Optional.of(filter))) {
      return;
    }

    final KuduSortRel newSort = new KuduSortRel(project.getCluster(), traitSet.replace(KuduRelNode.CONVENTION),
        convert(project, project.getTraitSet().replace(RelCollations.EMPTY)), newCollation, originalSort.offset,
        originalSort.fetch, true, Lists.newArrayList());

    // Copy in the new collation because! this new rel is now coming out Sorted.
    final RelNode newkuduToEnumerableRel = kuduToEnumerableRel
        .copy(kuduToEnumerableRel.getTraitSet().replace(newCollation), Lists.newArrayList(newSort));

    // Create a the aggregation relation and indicate it is sorted based on result.
    // Add in our sorted collation into the aggregation as the input:
    // kuduToEnumerable
    // is coming out sorted because the kudu sorted rel is enabling it.
    final RelNode newAggregation = originalAggregate.copy(
        originalAggregate.getTraitSet().replace(originalSort.getCollation()),
        Collections.singletonList(newkuduToEnumerableRel));

    call.transformTo(newAggregation);
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    final Sort originalSort = (Sort) call.getRelList().get(0);
    final Aggregate originalAggregate = (Aggregate) call.getRelList().get(1);
    final KuduToEnumerableRel kuduToEnumerableRel = (KuduToEnumerableRel) call.getRelList().get(2);
    final Project project = (Project) call.getRelList().get(3);
    final Filter filter = (Filter) call.getRelList().get(4);
    final KuduQuery query = (KuduQuery) call.getRelList().get(5);

    perform(call, originalSort, originalAggregate, kuduToEnumerableRel, project, filter, query);
  }

}
