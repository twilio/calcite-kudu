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
package com.twilio.kudu.sql.rel;

import com.twilio.kudu.sql.KuduEnumerable;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.JoinType;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.BlockBuilder;
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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.twilio.kudu.sql.KuduMethod;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implements a join algorithm between two {@link KuduToEnumerableRel} objects
 * using
 * {@link EnumerableDefaults#correlateBatchJoin(JoinType, Enumerable, Function1, Function2, Predicate2, int)}
 */
public class KuduNestedJoin extends Join implements EnumerableRel {

  final int batchSize;

  protected KuduNestedJoin(final RelOptCluster cluster, final RelTraitSet traits, final RelNode left,
      final RelNode right, final RexNode condition, final JoinRelType joinType, final int batchSize) {
    super(cluster, traits, left, right, condition, Collections.emptySet(), joinType);
    this.batchSize = batchSize;
  }

  public static KuduNestedJoin create(final RelNode left, final RelNode right, final RexNode condition,
      final JoinRelType joinType, final int batchSize) {
    final RelOptCluster cluster = left.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    // Sets Enumerable trait and *IF* left is sorted, preserve that sort.
    final RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE).replaceIfs(
        RelCollationTraitDef.INSTANCE, () -> RelMdCollation.enumerableBatchNestedLoopJoin(mq, left, right, joinType));
    return new KuduNestedJoin(cluster, traitSet, left, right, condition, joinType, batchSize);
  }

  @Override
  public KuduNestedJoin copy(final RelTraitSet traitSet, final RexNode condition, final RelNode left,
      final RelNode right, final JoinRelType joinType, final boolean semiJoinDone) {
    return new KuduNestedJoin(getCluster(), traitSet, left, right, condition, joinType, this.batchSize);
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
    double dRows = 0;
    double dCpu = 0;
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  @Override
  public RelWriter explainTerms(final RelWriter pw) {
    super.explainTerms(pw);
    return pw.item("batchSize", batchSize);
  }

  @Override
  public Result implement(final EnumerableRelImplementor implementor, final Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final Result leftResult = implementor.visitChild(this, 0, (EnumerableRel) left, pref);
    final Expression leftExpression = builder.append("left", leftResult.block);

    final Result rightResult = implementor.visitChild(this, 1, (EnumerableRel) right, pref);

    final Expression rightExpression = builder.append("right", rightResult.block);

    final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(),
        pref.prefer(JavaRowFormat.CUSTOM));
    final Expression selector = joinSelector(joinType, physType,
        ImmutableList.of(leftResult.physType, rightResult.physType));

    final Expression predicate = generatePredicate(implementor, getCluster().getRexBuilder(), left, right,
        leftResult.physType, rightResult.physType, condition);

    builder.append(Expressions.call(BuiltInMethod.CORRELATE_BATCH_JOIN.method,
        Expressions.constant(toLinq4jJoinType(joinType)), leftExpression,
        Expressions.call(Expressions.convert_(rightExpression, KuduEnumerable.class),
            KuduMethod.NESTED_JOIN_PREDICATES.method, implementor.stash(this, Join.class)),
        selector, predicate, Expressions.constant(batchSize)));
    return implementor.result(physType, builder.toBlock());
  }

  /**
   * Copy and Pasta from {@link EnumUtils#generatePredicate}
   */
  private static Expression generatePredicate(final EnumerableRelImplementor implementor, final RexBuilder rexBuilder,
      final RelNode left, final RelNode right, final PhysType leftPhysType, final PhysType rightPhysType,
      final RexNode condition) {
    final BlockBuilder builder = new BlockBuilder();
    final ParameterExpression left_ = Expressions.parameter(leftPhysType.getJavaRowType(), "left");
    final ParameterExpression right_ = Expressions.parameter(rightPhysType.getJavaRowType(), "right");
    final RexProgramBuilder program = new RexProgramBuilder(implementor.getTypeFactory().builder()
        .addAll(left.getRowType().getFieldList()).addAll(right.getRowType().getFieldList()).build(), rexBuilder);
    program.addCondition(condition);
    builder.add(Expressions.return_(null,
        RexToLixTranslator.translateCondition(program.getProgram(), implementor.getTypeFactory(), builder,
            new RexToLixTranslator.InputGetterImpl(
                ImmutableList.of(Pair.of(left_, leftPhysType), Pair.of(right_, rightPhysType))),
            implementor::getCorrelVariableGetter, implementor.getConformance())));
    return Expressions.lambda(Predicate2.class, builder.toBlock(), left_, right_);
  }

  /**
   * Copy and Pasta from {@link EnumUtils#toLinq4jJoinType}
   */
  private static JoinType toLinq4jJoinType(final JoinRelType joinRelType) {
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

  /**
   * Copy and Pasta from {@link EnumUtils#joinSelector}
   */
  private static Expression joinSelector(JoinRelType joinType, PhysType physType, List<PhysType> inputPhysTypes) {
    // A parameter for each input.
    final List<ParameterExpression> parameters = new ArrayList<>();

    // Generate all fields.
    final List<Expression> expressions = new ArrayList<>();
    final int outputFieldCount = physType.getRowType().getFieldCount();
    for (Ord<PhysType> ord : Ord.zip(inputPhysTypes)) {
      final PhysType inputPhysType = ord.e.makeNullable(joinType.generatesNullsOn(ord.i));
      // If input item is just a primitive, we do not generate specialized
      // primitive apply override since it won't be called anyway
      // Function<T> always operates on boxed arguments
      final ParameterExpression parameter = Expressions.parameter(Primitive.box(inputPhysType.getJavaRowType()),
          EnumUtils.LEFT_RIGHT.get(ord.i));
      parameters.add(parameter);
      if (expressions.size() == outputFieldCount) {
        // For instance, if semi-join needs to return just the left inputs
        break;
      }
      final int fieldCount = inputPhysType.getRowType().getFieldCount();
      for (int i = 0; i < fieldCount; i++) {
        Expression expression = inputPhysType.fieldReference(parameter, i,
            physType.getJavaFieldType(expressions.size()));
        if (joinType.generatesNullsOn(ord.i)) {
          expression = Expressions.condition(Expressions.equal(parameter, Expressions.constant(null)),
              Expressions.constant(null), expression);
        }
        expressions.add(expression);
      }
    }
    return Expressions.lambda(Function2.class, physType.record(expressions), parameters);
  }
}
