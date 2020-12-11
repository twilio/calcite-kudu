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

import java.util.Optional;

import com.twilio.kudu.sql.KuduQuery;
import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.rel.KuduSortRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
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
 * Two Sort Rules that look to push the Sort into the Kudu RPC.
 */
public abstract class KuduSortRule extends RelOptRule {

  private static final RelOptRuleOperand SIMPLE_OPERAND = operand(KuduQuery.class, none());

  private static final RelOptRuleOperand FILTER_OPERAND = operand(Filter.class, some(operand(KuduQuery.class, none())));

  public static final RelOptRule SIMPLE_SORT_RULE = new KuduSortWithoutFilter(RelFactories.LOGICAL_BUILDER);
  public static final RelOptRule FILTER_SORT_RULE = new KuduSortWithFilter(RelFactories.LOGICAL_BUILDER);

  public KuduSortRule(RelOptRuleOperand operand, RelBuilderFactory factory, String description) {
    super(operand, factory, description);
  }

  public boolean canApply(final RelTraitSet sortTraits, final KuduQuery query, final KuduTable openedTable,
      final Optional<Filter> filter) {
    // If there is no sort -- i.e. there is only a limit
    // don't pay the cost of returning rows in sorted order.
    final RelCollation collation = sortTraits.getTrait(RelCollationTraitDef.INSTANCE);

    if (collation.getFieldCollations().isEmpty()) {
      return false;
    }

    if (sortTraits.contains(KuduRelNode.CONVENTION)) {
      return false;
    }

    int pkColumnIndex = 0;

    for (final RelFieldCollation sortField : collation.getFieldCollations()) {
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
      // the sort columns must be a prefix of the primary key columns
      if (sortField.getFieldIndex() >= openedTable.getSchema().getPrimaryKeyColumnCount()
          || sortField.getFieldIndex() != pkColumnIndex) {
        // This field is not in the primary key columns. If there is a condition lets
        // see if it is there
        if (filter.isPresent()) {
          final RexNode originalCondition = filter.get().getCondition();
          while (pkColumnIndex < sortField.getFieldIndex()) {
            final KuduFilterVisitor visitor = new KuduFilterVisitor(pkColumnIndex);
            final Boolean foundFieldInCondition = originalCondition.accept(visitor);
            if (foundFieldInCondition.equals(Boolean.FALSE)) {
              return false;
            }
            pkColumnIndex++;
          }
        } else {
          return false;
        }
      }
      pkColumnIndex++;
    }
    return true;
  }

  public void perform(final RelOptRuleCall call, final Sort originalSort, final KuduQuery query,
      final KuduTable openedTable, final Optional<Filter> filter) {
    if (canApply(originalSort.getTraitSet(), query, openedTable, filter)) {
      final RelNode input = originalSort.getInput();
      final RelTraitSet traitSet = originalSort.getTraitSet().replace(KuduRelNode.CONVENTION)
          .replace(originalSort.getCollation());
      final RelNode newNode = new KuduSortRel(input.getCluster(), traitSet,
          convert(input, traitSet.replace(RelCollations.EMPTY)), originalSort.getCollation(), originalSort.offset,
          originalSort.fetch);
      call.transformTo(newNode);
    }
  }

  /**
   * Rule to match a Sort above {@link KuduQuery}. Applies only if sort matches
   * primary key order. Can match descending sorted tables as well.
   * {@see KuduQuery#descendingSortedFieldIndices}
   */
  public static class KuduSortWithoutFilter extends KuduSortRule {

    public KuduSortWithoutFilter(final RelBuilderFactory factory) {
      super(operand(Sort.class, SIMPLE_OPERAND), factory, "KuduSort: Simple");
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
      final KuduQuery query = (KuduQuery) call.getRelList().get(1);
      final KuduTable openedTable = query.calciteKuduTable.getKuduTable();
      final Sort originalSort = (Sort) call.getRelList().get(0);

      perform(call, originalSort, query, openedTable, Optional.<Filter>empty());
    }
  }

  /**
   * Rule to match a Sort above {@link Filter} and it is above {@link KuduQuery}.
   * Applies if sort matches primary key or if the primary key is required in the
   * filter. Can match descending sorted tables as well
   * {@see KuduQuery#descendingSortedFieldIndices}
   */
  public static class KuduSortWithFilter extends KuduSortRule {
    public KuduSortWithFilter(final RelBuilderFactory factory) {
      super(operand(Sort.class, FILTER_OPERAND), factory, "KuduSort: Filters");
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
      final KuduQuery query = (KuduQuery) call.getRelList().get(2);
      final Filter filter = (Filter) call.getRelList().get(1);
      final KuduTable openedTable = query.calciteKuduTable.getKuduTable();
      final Sort originalSort = (Sort) call.getRelList().get(0);

      perform(call, originalSort, query, openedTable, Optional.of(filter));
    }
  }

  /**
   * Searches {@link RexNode} to see if the Kudu column index -- stored as
   * {@link mustHave} is present in the {@code RexNode} and is required. Currently
   * does not handle OR clauses.
   */
  public static class KuduFilterVisitor extends RexVisitorImpl<Boolean> {
    public final int mustHave;

    public KuduFilterVisitor(final int mustHave) {
      super(true);
      this.mustHave = mustHave;
    }

    public Boolean visitInputRef(final RexInputRef inputRef) {
      return inputRef.getIndex() == this.mustHave;
    }

    public Boolean visitLocalRef(final RexLocalRef localRef) {
      return Boolean.FALSE;
    }

    public Boolean visitLiteral(final RexLiteral literal) {
      return Boolean.FALSE;
    }

    public Boolean visitCall(final RexCall call) {
      switch (call.getOperator().getKind()) {
      case EQUALS:
        return call.operands.get(0).accept(this);
      case AND:
        for (final RexNode operand : call.operands) {
          if (operand.accept(this).equals(Boolean.TRUE)) {
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
