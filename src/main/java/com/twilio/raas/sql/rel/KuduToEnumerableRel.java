package com.twilio.raas.sql.rel;

import com.twilio.raas.sql.CalciteKuduTable;
import com.twilio.raas.sql.KuduMethod;
import com.twilio.raas.sql.KuduPhysType;
import com.twilio.raas.sql.KuduRelNode;
import com.twilio.raas.sql.KuduScanStats;
import com.twilio.raas.sql.KuduWrite;
import com.twilio.raas.sql.rules.KuduToEnumerableConverter;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.kudu.client.RowResult;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class KuduToEnumerableRel extends ConverterImpl  implements EnumerableRel {
    public KuduToEnumerableRel(RelOptCluster cluster,
                                  RelTraitSet traits,
                                  RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new KuduToEnumerableRel(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      if (input instanceof KuduWrite) {
        return executeMutation(implementor, pref);
      } else {
        return executeQuery(implementor, pref);
      }
    }

    /**
     * This does the bulk of the work, creating a compile-able query to execute.
     */
    private Result executeQuery(EnumerableRelImplementor implementor, Prefer pref) {
      // Generates a call to "query" with the appropriate fields and predicates
      final BlockBuilder list = new BlockBuilder();
      final KuduRelNode.Implementor kuduImplementor = new KuduRelNode.Implementor();
      // This goes and visits the entire tree, setting up kuduImplementor
      // with predicates, fields and limits.
      kuduImplementor.visitChild(0, getInput());
      final RelDataType rowType = getRowType();
      final PhysType physType =
          PhysTypeImpl.of(
                          implementor.getTypeFactory(), rowType,
                          pref.prefer(JavaRowFormat.ARRAY));

      // Now build the Java code that represents the Physical scan of a
      // Kudu Table.
      final Expression predicates = list.append("predicates",
          implementor.stash(kuduImplementor.predicates, List.class));

      // @TODO: for correlation variables, for $batchSize,
      // acquire the InputGetter("$cor" + i). Then for all fields, call
      // the InputGetter.field() for that j.
      // and turn those into CalciteKuduPredicates.

      final Expression fields =
          list.append("kuduFields",
              implementor.stash(kuduImplementor.kuduProjectedColumns, List.class));

      final Expression limit =
              list.append("limit",
                      Expressions.constant(kuduImplementor.limit));

      final Expression offset =
              list.append("offset",
                      Expressions.constant(kuduImplementor.offset));

      final Expression sorted =
              list.append("sorted",
                      Expressions.constant(kuduImplementor.sorted));

      final Expression table =
          list.append("table",
                  kuduImplementor.table.getExpression(CalciteKuduTable.KuduQueryable.class));

      final Expression scanStats =
          list.append("scanStats", implementor.stash(new KuduScanStats(), KuduScanStats.class));

      final Expression cancelBoolean = list.append("cancelBoolean",
          Expressions.convert_(
                  Expressions.call(DataContext.ROOT,
                      BuiltInMethod.DATA_CONTEXT_GET.method,
                      Expressions.constant(DataContext.Variable.CANCEL_FLAG.camelName)),
                  AtomicBoolean.class)
      );

      final Expression mapFunction;
      if (kuduImplementor.projections != null && !kuduImplementor.projections.isEmpty()) {
          final BlockBuilder projectExpressionBlock = new BlockBuilder();

          final PhysType tablePhystype = new KuduPhysType(kuduImplementor.kuduTable.getSchema(), kuduImplementor.tableDataType,
              kuduImplementor.descendingColumns, kuduImplementor.kuduProjectedColumns);
          final ParameterExpression inputRow = Expressions.parameter(Object.class);

          List<Pair<RexNode, String>> namedProjects = Pair.zip(kuduImplementor.projections, getRowType().getFieldNames());
          final RexProgramBuilder builder = new RexProgramBuilder(tablePhystype.getRowType(), getCluster().getRexBuilder());

          namedProjects
              .stream()
              .forEach(pair -> builder.addProject(pair.left, pair.right));


          final Expression castToRow = Expressions.convert_(inputRow, RowResult.class);

          final InputGetter inputGetter = new RexToLixTranslator.InputGetterImpl(
              Collections.singletonList(Pair.of(castToRow, tablePhystype)));
          final RexProgram projectionFunctions = builder.getProgram();
          final List<Expression> projectionExpressions = RexToLixTranslator.translateProjects(projectionFunctions, implementor.getTypeFactory(),
              implementor.getConformance(), projectExpressionBlock, tablePhystype, DataContext.ROOT, inputGetter, null);

          projectExpressionBlock.add(Expressions.return_(null, physType.record(projectionExpressions)));

          mapFunction = Expressions.new_(
              Function1.class,
              Collections.emptyList(),
              Expressions.methodDecl(Modifier.PUBLIC,
                  Object.class,
                  "apply",
                  Collections.singletonList(inputRow),
                  projectExpressionBlock.toBlock()
              )
          );
      }
      else {
          mapFunction = Expressions.constant(null);
      }

      Expression enumerable = list.append("enumerable",
              Expressions.call(table, KuduMethod.KUDU_QUERY_METHOD.method, predicates, fields, limit, offset, sorted,
                  Expressions.constant(kuduImplementor.groupByLimited), scanStats, cancelBoolean, mapFunction));

      Hook.QUERY_PLAN.run(predicates);
      list.add(
               Expressions.return_(null, enumerable));

      KuduToEnumerableConverter.logger.debug("Created a KuduQueryable " + list.toBlock());
      return implementor.result(physType, list.toBlock());
    }

  private Result executeMutation(EnumerableRelImplementor implementor, Prefer prefer) {
    // Generates a call to "mutate" with the appropriate parameters
    final BlockBuilder list = new BlockBuilder();
    final RelDataType rowType = getRowType();
    final PhysType physType =
      PhysTypeImpl.of(implementor.getTypeFactory(), rowType, prefer.preferArray());

    final KuduRelNode.Implementor kuduImplementor = new KuduRelNode.Implementor();
    // This goes and visits the entire tree, setting up kuduImplementor
    // with the columnNames and tuples
    kuduImplementor.visitChild(0, getInput());

    // Now build the Java code to execute the mutation
    final Expression columnNames = list.append("columnIndexes",
      implementor.stash(kuduImplementor.columnIndexes, List.class));

    final Expression table =
      list.append("table",
        kuduImplementor.table.getExpression(CalciteKuduTable.KuduQueryable.class));


    if (kuduImplementor.numBindExpressions != 0) {
      // a PreparedStatement was used
      final Expression values =
        list.append("values", valuesArrayList(list, kuduImplementor.numBindExpressions));
      Expression enumerable = list.append("enumerable",
        Expressions.call(table,
          KuduMethod.KUDU_MUTATE_ROW_METHOD.method, columnNames, values));
      list.add(
        Expressions.return_(null, enumerable));
    }
    else {
      // a regular Statement was used
      final Expression tuples = list.append("tuples",
        implementor.stash(kuduImplementor.tuples, List.class));
      Expression enumerable = list.append("enumerable",
        Expressions.call(table,
          KuduMethod.KUDU_MUTATE_TUPLES_METHOD.method, columnNames, tuples));
      list.add(
        Expressions.return_(null, enumerable));
    }

    KuduToEnumerableConverter.logger.debug("Created a KuduMutation " + list.toBlock());
    return implementor.result(physType, list.toBlock());
  }

  private static MethodCallExpression valuesArrayList(final BlockBuilder list,
                                                      final int numBindExpressions) {
    return Expressions.call(
      BuiltInMethod.ARRAYS_AS_LIST.method,
      Expressions.newArrayInit(Object.class, bindExpressions(list, numBindExpressions)));
  }

  private static List<Expression> bindExpressions(final BlockBuilder list,
                                                  final int numBindExpressions) {
    List<Expression> expressionList = new ArrayList<>();
    for (int index = 0; index < numBindExpressions; ++index) {
      String bindExpressionName = "bind" + index;
      // get the value to be bound from the DataContext
      expressionList.add(list.append(bindExpressionName, Expressions.call(DataContext.ROOT,
        BuiltInMethod.DATA_CONTEXT_GET.method, Expressions.constant("?" + index))));
    }
    return expressionList;
  }

}
