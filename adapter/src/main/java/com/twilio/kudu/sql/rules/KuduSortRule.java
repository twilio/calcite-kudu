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

import com.twilio.kudu.sql.KuduQuery;
import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.rel.KuduSortRel;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.kudu.client.KuduTable;

/**
 * Two Sort Rules that look to push the Sort into the Kudu RPC.
 */
public class KuduSortRule extends RelOptRule {

  public static final RelOptRule INSTANCE = new KuduSortRule(operand(Sort.class, any()), RelFactories.LOGICAL_BUILDER,
      "KuduSortRule");

  public KuduSortRule(RelOptRuleOperand operand, RelBuilderFactory factory, String description) {
    super(operand, factory, description);
  }

  protected boolean canApply(final Sort original) {
    final FindKuduQuery finder = new FindKuduQuery();
    original.childrenAccept(finder);
    final KuduQuery query = finder.foundQuery;
    if (query == null || finder.tooManyChoices) {
      return false;
    }
    final KuduTable openedTable = query.calciteKuduTable.getKuduTable();
    final RelCollation collation = original.getCollation();

    final RelMetadataQuery mq = query.getCluster().getMetadataQuery();
    RelOptPredicateList predicates = null;

    if (collation.getFieldCollations().isEmpty()) {
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

      // If the sorted field isn't in the primary keys the rule can still be applied
      // if and only if the sorted field is strictly filtered so there is only one
      // value.
      if (sortField.getFieldIndex() >= openedTable.getSchema().getPrimaryKeyColumnCount()) {
        // This is duplicated code but duplicated on purpose. Calculating predicates is
        // expensive and should be done once and only if required.
        if (predicates == null) {
          predicates = mq.getAllPredicates(original);
          if (predicates == null) {
            return false;
          }
        }

        final Boolean nonPkIsFiltered = isColumnStrictlyFiltered(predicates, mq, original, sortField.getFieldIndex());
        if (!nonPkIsFiltered) {
          return false;
        }
      } else {
        // Iterate through all the primary key columns below this one and assert that
        // all of them have
        // strict filter on them. If one of them does don not apply the rule by
        // returning FALSE
        while (sortField.getFieldIndex() > pkColumnIndex) {
          // This is duplicated code but duplicated on purpose. Calculating predicates is
          // expensive
          // and should be done once and only if required.
          if (predicates == null) {
            predicates = mq.getAllPredicates(original);
            if (predicates == null) {
              return false;
            }
          }
          final boolean primaryKeyIncluded = isColumnStrictlyFiltered(predicates, mq, original, pkColumnIndex);
          if (!primaryKeyIncluded) {
            return false;
          } else {
            pkColumnIndex++;
          }
        }
      }
      pkColumnIndex++;
    }
    return true;
  }

  private boolean isColumnStrictlyFiltered(final RelOptPredicateList predicates, final RelMetadataQuery mq,
      final RelNode original, final int column) {
    final KuduFilterVisitor visitor = new KuduFilterVisitor(column);
    return predicates.pulledUpPredicates.stream().anyMatch(rexNode -> {
      final Boolean matched = rexNode.accept(visitor);
      return matched != null && matched;
    });
  }

  /**
   * Visits the entire RelNode tree to find the {@link KuduQuery}
   */
  static class FindKuduQuery extends RelVisitor {
    KuduQuery foundQuery = null;
    boolean tooManyChoices = false;

    @Override
    public void visit(RelNode rel, int ordinal, RelNode parent) {
      if (rel instanceof KuduQuery) {
        if (foundQuery == null) {
          this.foundQuery = (KuduQuery) rel;
        } else {
          // Parsed a JOIN with multiple KuduQueries. Await for the join to be pushed into
          // the Join
          tooManyChoices = true;
        }

      } else if (rel instanceof RelSubset) {
        final RelSubset node = (RelSubset) rel;
        if (node.getBest() != null) {
          visit(node.getBest(), ordinal, parent);
        } else if (node.getOriginal() != null) {
          visit(node.getOriginal(), ordinal, parent);
        }
      } else {
        super.visit(rel, ordinal, parent);
      }
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

    @Override
    public Boolean visitInputRef(final RexInputRef inputRef) {
      return inputRef.getIndex() == this.mustHave;
    }

    /**
     * This type of {@link RexNode} is returned by
     * {@link RelMetadataQuery#getAllPredicates(RelNode)} and it contains
     * information on the table as well. Treat it the same as an {@link RexInputRef}
     */
    @Override
    public Boolean visitTableInputRef(RexTableInputRef tableRef) {
      return visitInputRef(tableRef);
    }

    @Override
    public Boolean visitLocalRef(final RexLocalRef localRef) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean visitLiteral(final RexLiteral literal) {
      return Boolean.FALSE;
    }

    @Override
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

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Sort original = (Sort) call.getRelList().get(0);

    if (original.getConvention() != KuduRelNode.CONVENTION && !canApply(original)) {
      return;
    }
    final RelNode input = original.getInput();
    final RelTraitSet traitSet = original.getTraitSet().replace(KuduRelNode.CONVENTION)
        .replace(original.getCollation());
    final RelNode newNode = new KuduSortRel(input.getCluster(), traitSet,
        convert(input, traitSet.replace(RelCollations.EMPTY).replace(KuduRelNode.CONVENTION)), original.getCollation(),
        original.offset, original.fetch);
    call.transformTo(newNode);
  }
}
