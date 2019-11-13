package com.twilio.raas.sql.rules;

import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.plan.RelOptRuleCall;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rel.logical.LogicalJoin;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rel.RelNode;

import static org.apache.calcite.plan.RelOptUtil.conjunctions;

public class OuterToInnerJoinRule extends RelOptRule {
  public OuterToInnerJoinRule(RelBuilderFactory relBuilder) {
    super(
        operand(Filter.class,
            operand(Join.class, RelOptRule.any())),
        relBuilder,
        "OuterToInnerJoinRule");
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Join join = call.rel(1);

    // Copy and Pasta from FilterJoinRule
    final List<RexNode> conjunctions = conjunctions(filter.getCondition());
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    for (int i = 0; i < conjunctions.size(); i++) {
      RexNode node = conjunctions.get(i);
      if (node instanceof RexCall) {
        conjunctions.set(i,
            RelOptUtil.collapseExpandedIsNotDistinctFromExpr((RexCall) node, rexBuilder));
      }
    }
    // End Copy and Pasta from FilterJoinRule

    JoinRelType joinType = join.getJoinType();
    if (!joinType.generatesNullsOnLeft() &&
        !joinType.generatesNullsOnRight()) {
      return;
    }

    final int nFieldsLeft = join
      .getInputs()
      .get(0)
      .getRowType()
      .getFieldList()
      .size();
    final int nFieldsRight = join
      .getInputs()
      .get(1)
      .getRowType()
      .getFieldList()
      .size();

    final int nTotalFields = join.getRowType().getFieldList().size();
    final int nSysFields = 0; // joinRel.getSystemFieldList().size();

    final ImmutableBitSet leftBitmap = ImmutableBitSet.range(
        nSysFields, nSysFields + nFieldsLeft);
    final ImmutableBitSet rightBitmap = ImmutableBitSet.range(
        nSysFields + nFieldsLeft, nTotalFields);

    if (joinType.generatesNullsOnRight()) {
      final RexVisitor<Boolean> inspectRightSide = new TableNotNullable(rightBitmap);
      final Boolean inspectionResult = filter.getCondition().accept(inspectRightSide);
      if (inspectionResult != null && inspectionResult == Boolean.TRUE) {
        joinType = joinType.cancelNullsOnRight();
      }
    }
    if (joinType.generatesNullsOnLeft()) {
      final RexVisitor<Boolean> inspectLeftSide = new TableNotNullable(leftBitmap);
      final Boolean inspectionResult = filter.getCondition().accept(inspectLeftSide);
      if (inspectionResult != null && inspectionResult == Boolean.TRUE) {
        joinType = joinType.cancelNullsOnLeft();
      }
    }

    if (joinType == join.getJoinType()) {
      return;
    }

    final RelBuilder relBuilder = call.builder();
    final RelNode leftRel =
      relBuilder.push(join.getLeft()).build();
    final RelNode rightRel =
      relBuilder.push(join.getRight()).build();

    final List<RexNode> joinFilters =
      RelOptUtil.conjunctions(join.getCondition());

    // create the new join node referencing the new children and
    // containing its new join filters (if there are any)
    final ImmutableList<RelDataType> fieldTypes =
      ImmutableList.<RelDataType>builder()
      .addAll(RelOptUtil.getFieldTypeList(leftRel.getRowType()))
      .addAll(RelOptUtil.getFieldTypeList(rightRel.getRowType())).build();
    final RexNode joinFilter =
      RexUtil.composeConjunction(rexBuilder,
          RexUtil.fixUp(rexBuilder, joinFilters, fieldTypes));

    RelNode newJoinRel =
      join.copy(
          join.getTraitSet(),
          join.getCondition(),
          join.getLeft(),
          join.getRight(),
          joinType.cancelNullsOnRight(),
          join.isSemiJoinDone());

    call.getPlanner().onCopy(join, newJoinRel);
    relBuilder.push(newJoinRel);
    relBuilder.convert(join.getRowType(), false);

    // create a FilterRel on top of the join.
    relBuilder.filter(
        RexUtil.fixUp(rexBuilder, conjunctions,
            RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));
    call.transformTo(relBuilder.build());
  }


    /**
   * Visits RexNodes to determine if -- for this filter -- the table on the side of the join can be
   * nullable. Returns true if the results from the table must be present in the result set.
   */
  public static class TableNotNullable extends RexVisitorImpl<Boolean> {
    private boolean visitingNegation = false;
    public final ImmutableBitSet tableFields;
    public TableNotNullable(final ImmutableBitSet tableFields) {
      super(true);
      this.tableFields = tableFields;
    }
    @Override public Boolean visitCall(RexCall call) {
      final SqlKind callType = call.getOperator().getKind();
      switch (callType) {
      case NOT:
      case NOT_IN:
      case NOT_EQUALS:
      case IS_NULL:
        visitingNegation = true;
        break;
      case AND:
        // Processing an AND, assume that this won't pass and if any of the operands returns true
        // then return true for this RexCall.
        return call.operands
          .stream()
          .map(rexNode -> rexNode.accept(this))
          .reduce(false,
              (left, right) -> {
                if (right == null) {
                  return left;
                }
                return left || right;
              });
      case OR:
        // Processing an OR, assume that it does pass and require all operands to return true
        // then return true for this RexCall.
        return call.operands
          .stream()
          .map(rexNode -> rexNode.accept(this))
          .reduce(false,
              (left, right) -> {
                if (right == null) {
                  return false;
                }
                return left || right;
              });
      case IS_NOT_NULL:
        return call.operands
          .stream()
          .map(rexNode -> rexNode.accept(this))
          .reduce(false,
              (left, right) -> {
                if (right == null) {
                  return left;
                }
                return left && right;
              });
      default:
        visitingNegation = false;
      }
      boolean mustMatchSide = false;
      for (RexNode operand: call.operands) {
        final Boolean childCall = operand.accept(this);
        if (childCall != null) {
          if (childCall == Boolean.FALSE) {
            return Boolean.FALSE;
          }
          mustMatchSide = true;
        }
      }
      return mustMatchSide;
    }

    @Override public Boolean visitInputRef(RexInputRef input) {
      if (tableFields.indexOf(input.getIndex()) >= 0) {
        return !visitingNegation;
      }
      return false;
    }
  }
}
