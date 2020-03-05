package com.twilio.raas.sql.rel;

import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.DeclarationStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.twilio.raas.sql.KuduMethod;
import com.twilio.raas.sql.SortableEnumerable;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/** Implementation of batch nested loop join in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class KuduNestedJoin extends Join implements EnumerableRel {

  protected KuduNestedJoin(
      final RelOptCluster cluster,
      final RelTraitSet traits,
      final RelNode left,
      final RelNode right,
      final RexNode condition,
      final JoinRelType joinType) {
    super(cluster, traits, left, right, condition, Collections.emptySet(), joinType);
  }

  public static KuduNestedJoin create(
      final RelNode left,
      final RelNode right,
      final RexNode condition,
      final JoinRelType joinType) {
    final RelOptCluster cluster = left.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.enumerableBatchNestedLoopJoin(mq, left, right, joinType));
    return new KuduNestedJoin(
        cluster,
        traitSet,
        left,
        right,
        condition,
        joinType);
  }

  @Override public KuduNestedJoin copy(final RelTraitSet traitSet,
      final RexNode condition, final RelNode left, final RelNode right, final JoinRelType joinType,
      final boolean semiJoinDone) {
    return new KuduNestedJoin(getCluster(), traitSet,
        left, right, condition, joinType);
  }

  @Override public RelOptCost computeSelfCost(
      final RelOptPlanner planner,
      final RelMetadataQuery mq) {
    final double rowCount = mq.getRowCount(this);

    final double rightRowCount = right.estimateRowCount(mq);
    final double leftRowCount = left.estimateRowCount(mq);
    if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    final Double restartCount = mq.getRowCount(getLeft()) / variablesSet.size();

    final RelOptCost rightCost = planner.getCost(getRight(), mq);
    final RelOptCost rescanCost =
        rightCost.multiplyBy(Math.max(1.0, restartCount - 1));

    // TODO Add cost of last loop (the one that looks for the match)
    return planner.getCostFactory().makeCost(
        rowCount + leftRowCount, 0, 0).plus(rescanCost);
  }

  @Override public RelWriter explainTerms(final RelWriter pw) {
    super.explainTerms(pw);
    return pw.item("batchSize", variablesSet.size());
  }

  @Override public Result implement(final EnumerableRelImplementor implementor, final Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final Result leftResult =
        implementor.visitChild(this, 0, (EnumerableRel) left, pref);
    final Expression leftExpression =
        builder.append(
            "left", leftResult.block);

    final Result rightResult =
        implementor.visitChild(this, 1, (EnumerableRel) right, pref);

    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM));
    final Expression selector =
        joinSelector(
            joinType, physType,
            ImmutableList.of(leftResult.physType, rightResult.physType));

    final Expression predicate =
        generatePredicate(implementor, getCluster().getRexBuilder(), left, right,
            leftResult.physType, rightResult.physType, condition);

    builder.append(
        Expressions.call(BuiltInMethod.CORRELATE_BATCH_JOIN.method,
            Expressions.constant(toLinq4jJoinType(joinType)),
            leftExpression,
            Expressions.call(
                KuduMethod.NESTED_JOIN_PREDICATES.method,
                Expressions.constant(this)
            ),
            selector,
            predicate,
            Expressions.constant(variablesSet.size())));
    return implementor.result(physType, builder.toBlock());
  }

    /** Returns a predicate expression based on a join condition. **/
  static Expression generatePredicate(
      final EnumerableRelImplementor implementor,
      final RexBuilder rexBuilder,
      final RelNode left,
      final RelNode right,
      final PhysType leftPhysType,
      final PhysType rightPhysType,
      final RexNode condition) {
    final BlockBuilder builder = new BlockBuilder();
    final ParameterExpression left_ =
        Expressions.parameter(leftPhysType.getJavaRowType(), "left");
    final ParameterExpression right_ =
        Expressions.parameter(rightPhysType.getJavaRowType(), "right");
    final RexProgramBuilder program =
        new RexProgramBuilder(
            implementor.getTypeFactory().builder()
                .addAll(left.getRowType().getFieldList())
                .addAll(right.getRowType().getFieldList())
                .build(),
            rexBuilder);
    program.addCondition(condition);
    builder.add(
        Expressions.return_(null,
            RexToLixTranslator.translateCondition(program.getProgram(),
                implementor.getTypeFactory(),
                builder,
                new RexToLixTranslator.InputGetterImpl(
                    ImmutableList.of(Pair.of(left_, leftPhysType),
                        Pair.of(right_, rightPhysType))),
                implementor::getCorrelVariableGetter,
                implementor.getConformance())));
    return Expressions.lambda(Predicate2.class, builder.toBlock(), left_, right_);
  }
    /** Transforms a JoinRelType to Linq4j JoinType. **/
  static JoinType toLinq4jJoinType(final JoinRelType joinRelType) {
      switch (joinRelType) {
          case INNER:
              return JoinType.INNER;
          case LEFT:
              return JoinType.LEFT;
          case RIGHT:
              return JoinType.RIGHT;
          case FULL:
              return JoinType.FULL;
          case SEMI:
              return JoinType.SEMI;
          case ANTI:
              return JoinType.ANTI;
      }
      throw new IllegalStateException("Unable to convert " + joinRelType + " to Linq4j JoinType");
  }

    static Expression joinSelector(JoinRelType joinType, PhysType physType,
      List<PhysType> inputPhysTypes) {
    // A parameter for each input.
    final List<ParameterExpression> parameters = new ArrayList<>();

    // Generate all fields.
    final List<Expression> expressions = new ArrayList<>();
    final int outputFieldCount = physType.getRowType().getFieldCount();
    for (Ord<PhysType> ord : Ord.zip(inputPhysTypes)) {
      final PhysType inputPhysType =
          ord.e.makeNullable(joinType.generatesNullsOn(ord.i));
      // If input item is just a primitive, we do not generate specialized
      // primitive apply override since it won't be called anyway
      // Function<T> always operates on boxed arguments
      final ParameterExpression parameter =
          Expressions.parameter(Primitive.box(inputPhysType.getJavaRowType()),
              EnumUtils.LEFT_RIGHT.get(ord.i));
      parameters.add(parameter);
      if (expressions.size() == outputFieldCount) {
        // For instance, if semi-join needs to return just the left inputs
        break;
      }
      final int fieldCount = inputPhysType.getRowType().getFieldCount();
      for (int i = 0; i < fieldCount; i++) {
        Expression expression =
            inputPhysType.fieldReference(parameter, i,
                physType.getJavaFieldType(expressions.size()));
        if (joinType.generatesNullsOn(ord.i)) {
          expression =
              Expressions.condition(
                  Expressions.equal(parameter, Expressions.constant(null)),
                  Expressions.constant(null),
                  expression);
        }
        expressions.add(expression);
      }
    }
    return Expressions.lambda(
        Function2.class,
        physType.record(expressions),
        parameters);
  }
}
