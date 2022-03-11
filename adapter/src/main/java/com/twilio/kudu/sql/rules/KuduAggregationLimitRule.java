/* Copyright 2021 Twilio, Inc
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
import org.apache.calcite.adapter.enumerable.EnumerableLimitSort;
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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;
import org.apache.kudu.client.KuduTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Rule to match a limit over a sort over aggregation. There must be a common
 * prefix between the sort columns and the primary key columns of the table.
 *
 * If a table has three columns A, B and C with a PK(A, B) this rule can be
 * applied if sorting by (A, SUM(C)) and grouping by (A, B) with a limit. For
 * eg: (A,B,C) (1, 1, 4), (1, 2, 2), (2, 1, 1). If we query with a limit of 2,
 * then we can stop reading rows when we process the 3rd row as we will have 2
 * groups (1,1) and (1,2).
 * 
 * This rule limits the number of rows read from kudu, but rows still need to be
 * sorted on the client.
 */
public class KuduAggregationLimitRule extends RelOptRule {

  private static final RelOptRuleOperand OPERAND = operand(Sort.class,
      some(operand(Aggregate.class, some(operand(KuduToEnumerableRel.class,
          some(operand(Project.class, some(operand(Filter.class, some(operand(KuduQuery.class, none())))))))))));

  public static final RelOptRule AGGREGATION_LIMIT_RULE = new KuduAggregationLimitRule(RelFactories.LOGICAL_BUILDER);

  public KuduAggregationLimitRule(final RelBuilderFactory factory) {
    super(OPERAND, factory, "KuduAggregationWithLimit");
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    final Sort originalSort = (Sort) call.getRelList().get(0);
    // if there is no LIMIT just return
    if (originalSort.fetch == null)
      return;
    final Aggregate originalAggregate = (Aggregate) call.getRelList().get(1);
    final KuduToEnumerableRel kuduToEnumerableRel = (KuduToEnumerableRel) call.getRelList().get(2);
    final Project project = (Project) call.getRelList().get(3);
    boolean containsExpression = project.getProjects().stream().filter(n -> !(n instanceof RexInputRef)).findAny()
        .isPresent();
    if (containsExpression)
      return;
    final Filter filter = (Filter) call.getRelList().get(4);
    final KuduQuery query = (KuduQuery) call.getRelList().get(5);

    perform(call, originalSort, originalAggregate, kuduToEnumerableRel, project, filter, query);
  }

  protected void perform(final RelOptRuleCall call, final Sort originalSort, final Aggregate originalAggregate,
      final KuduToEnumerableRel kuduToEnumerableRel, final Project project, final Filter filter,
      final KuduQuery query) {

    // Check that the columns being grouped are a prefix of the pk columns
    int pkColumnIndex = 0;
    List<Integer> remappedGroupOrdinals = originalAggregate.getGroupSet().asList().stream()
        .map(groupedOrdinal -> ((RexInputRef) project.getProjects().get(groupedOrdinal)).getIndex())
        // sort so that we can check if the group by columns match a prefix
        .sorted().collect(Collectors.toList());

    final Map<Integer, Integer> remappingOrdinals = new HashMap<>();
    for (RelFieldCollation fieldCollation : originalSort.getCollation().getFieldCollations()) {
      int projectOrdinal = fieldCollation.getFieldIndex();
      // return if not sorting by a projected column
      if (projectOrdinal > project.getProjects().size() - 1) {
        return;
      }
      int kuduColumnIndex = ((RexInputRef) project.getProjects().get(projectOrdinal)).getIndex();
      remappingOrdinals.put(fieldCollation.getFieldIndex(), kuduColumnIndex);
    }

    RelCollation newCollation = RelCollationTraitDef.INSTANCE
        .canonize(RelCollations.permute(originalSort.getCollation(), remappingOrdinals));
    final RelTraitSet traitSet = originalSort.getTraitSet().plus(newCollation).plus(Convention.NONE);

    // TODO is this needed
    if (traitSet.contains(KuduRelNode.CONVENTION)) {
      return;
    }

    // Check the new trait set to see if we can apply the sort against this.
    final Pair<List<RelFieldCollation>, List<String>> pair = getSortPkPrefix(originalSort.getCollation(), newCollation,
        query, Optional.of(filter));
    if (pair.left.isEmpty()) {
      return;
    }

    // create a KuduSortRel but indicate that the input does not come out sorted so
    // that EnumerableSort is used to sort the results
    final KuduSortRel kuduSort = new KuduSortRel(project.getCluster(),
        originalSort.getTraitSet().replace(KuduRelNode.CONVENTION), project, originalSort.getCollation(),
        originalSort.offset, originalSort.fetch, true, pair.left, pair.right);

    final RelNode newKuduToEnumerableRel = kuduToEnumerableRel.copy(kuduToEnumerableRel.getTraitSet(),
        Lists.newArrayList(kuduSort));

    final RelNode newAggregation = originalAggregate.copy(originalAggregate.getTraitSet(),
        Collections.singletonList(newKuduToEnumerableRel));

    final RelNode limitSort = EnumerableLimitSort.create(newAggregation, originalSort.getCollation(),
        originalSort.offset, originalSort.fetch);

    call.transformTo(limitSort);
  }

  private boolean checkSortDirection(final KuduQuery query, final RelFieldCollation sortField) {
    // Reject for descending sorted fields if sort direction is not Descending
    if ((query.calciteKuduTable.isColumnOrderedDesc(sortField.getFieldIndex())
        && sortField.direction != RelFieldCollation.Direction.DESCENDING
        && sortField.direction != RelFieldCollation.Direction.STRICTLY_DESCENDING) ||
    // Else Reject if sort order is not ascending
        (!query.calciteKuduTable.isColumnOrderedDesc(sortField.getFieldIndex())
            && sortField.direction != RelFieldCollation.Direction.ASCENDING
            && sortField.direction != RelFieldCollation.Direction.STRICTLY_ASCENDING)) {
      return false;
    }
    return true;
  }

  public Pair<List<RelFieldCollation>, List<String>> getSortPkPrefix(final RelCollation originalCollation,
      final RelCollation newCollation, final KuduQuery query, final Optional<Filter> filter) {
    final KuduTable openedTable = query.calciteKuduTable.getKuduTable();
    final List<RelFieldCollation> sortPrefixColumns = Lists.newArrayList();
    final List<String> sortPkColumns = Lists.newArrayList();
    final Pair<List<RelFieldCollation>, List<String>> pair = new Pair<>(sortPrefixColumns, sortPkColumns);
    // If there is no sort just return
    if (newCollation.getFieldCollations().isEmpty()) {
      return pair;
    }

    int pkColumnIndex = 0;
    for (int collationIndex = 0; collationIndex < newCollation.getFieldCollations().size(); ++collationIndex) {
      final RelFieldCollation sortField = newCollation.getFieldCollations().get(collationIndex);
      if (!checkSortDirection(query, sortField)) {
        break;
      }
      // the sort columns must contain a prefix of the primary key columns
      // for eg if a table has three columns A, B and C with a PK(A, B) this rule can
      // be applied if we sort by (A, C)
      if (sortField.getFieldIndex() != pkColumnIndex) {
        // This field is not in the primary key columns. If there is a condition lets
        // see if it is there
        if (filter.isPresent()) {
          final RexBuilder rexBuilder = filter.get().getCluster().getRexBuilder();
          final RexNode originalCondition = RexUtil.expandSearch(rexBuilder, null, filter.get().getCondition());
          while (pkColumnIndex < sortField.getFieldIndex()) {
            final KuduSortRule.KuduFilterVisitor visitor = new KuduSortRule.KuduFilterVisitor(pkColumnIndex);
            final Boolean foundFieldInCondition = originalCondition.accept(visitor);
            if (foundFieldInCondition.equals(Boolean.FALSE)) {
              return pair;
            }
            pkColumnIndex++;
          }
        } else {
          break;
        }
      }

      // use the originalCollations
      sortPrefixColumns.add(originalCollation.getFieldCollations().get(collationIndex));
      String pkColumnName = openedTable.getSchema().getColumnByIndex(pkColumnIndex).getName();
      sortPkColumns.add(pkColumnName);
      pkColumnIndex++;
    }

    return pair;
  }

}
