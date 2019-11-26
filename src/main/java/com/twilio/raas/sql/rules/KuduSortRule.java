package com.twilio.raas.sql.rules;

import java.util.Optional;

import com.twilio.raas.sql.KuduQuery;
import com.twilio.raas.sql.KuduRel;
import com.twilio.raas.sql.rel.KuduFilterRel;
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
 * When a query is executed without a filter, check to see if the sort is in
 * Kudu Primary Key order. If so, tell Enumerable to sort results
 */
public abstract class KuduSortRule extends RelOptRule {

  public static final RelOptRuleOperand SIMPLE_OPERAND =
    operand(KuduToEnumerableRel.class,
        some(operand(KuduQuery.class, none())));

  public static final RelOptRuleOperand FILTER_OPERAND =
    operand(KuduToEnumerableRel.class, some(
            operand(KuduFilterRel.class, some(operand(KuduQuery.class, none())))));

  public static final RelOptRule SIMPLE_SORT_RULE = new KuduSortWithoutFilter(RelFactories.LOGICAL_BUILDER);
  public static final RelOptRule FILTER_SORT_RULE = new KuduSortWithFilter(RelFactories.LOGICAL_BUILDER);

  public KuduSortRule(final RelBuilderFactory relBuilderFactory, final String description, final RelOptRuleOperand operand) {
    super(operand(Sort.class, operand), relBuilderFactory,
        String.format("KuduSort: %s", description));
  }

  public void perform(final RelOptRuleCall call, final Sort originalSort, final KuduQuery query, final KuduTable openedTable, final Optional<Filter> filter) {
    // If there is no sort -- i.e. there is only a limit
    // don't pay the cost of returning rows in sorted order.
    if (originalSort.getCollation().getFieldCollations().isEmpty()) {
      return;
    }

    int mustMatch = 0;

    for (final RelFieldCollation sortField: originalSort.getCollation().getFieldCollations()) {
      // Reject for descending sorted fields if sort direction is not Descending
      if ((query.descendingSortedFieldIndices.contains(sortField.getFieldIndex()) &&
              sortField.direction != RelFieldCollation.Direction.DESCENDING &&
              sortField.direction != RelFieldCollation.Direction.STRICTLY_DESCENDING) ||
          // Else Reject if sort order is not ascending
          (!query.descendingSortedFieldIndices.contains(sortField.getFieldIndex()) &&
              sortField.direction != RelFieldCollation.Direction.ASCENDING &&
              sortField.direction != RelFieldCollation.Direction.STRICTLY_ASCENDING))
        {
          return;
        }
      if (sortField.getFieldIndex() >= openedTable.getSchema().getPrimaryKeyColumnCount() ||
          sortField.getFieldIndex() != mustMatch) {
        // This field is not in the primary key columns. If there is a condition lets see if it is there
        if (filter.isPresent()) {
          final RexNode originalCondition = filter.get().getCondition();
          while (mustMatch < sortField.getFieldIndex()) {
            final KuduFilterVisitor visitor = new KuduFilterVisitor(mustMatch);
            final Boolean foundFieldInCondition = originalCondition.accept(visitor);
            if (foundFieldInCondition == Boolean.FALSE) {
              return;
            }
            mustMatch++;
          }
        }
        else {
          return;
        }
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

  public static class KuduSortWithoutFilter extends KuduSortRule {

    public KuduSortWithoutFilter(final RelBuilderFactory factory) {
      super(factory, "WithoutFilter", SIMPLE_OPERAND);
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
      final KuduQuery query = (KuduQuery)call.getRelList().get(2);
      final KuduTable openedTable = query.openedTable;
      final Sort originalSort = (Sort)call.getRelList().get(0);

      perform(call, originalSort, query, openedTable, Optional.<Filter>empty());
    }
  }

  public static class KuduSortWithFilter extends KuduSortRule {
    public KuduSortWithFilter(final RelBuilderFactory factory) {
      super(factory, "WithFilter", FILTER_OPERAND);
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
      final KuduQuery query = (KuduQuery)call.getRelList().get(3);
      final Filter filter = (Filter) call.getRelList().get(2);
      final KuduTable openedTable = query.openedTable;
      final Sort originalSort = (Sort)call.getRelList().get(0);

      perform(call, originalSort, query, openedTable, Optional.of(filter));
    }
  }

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
                        if (operand.accept(this) == Boolean.TRUE) {
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
