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
import com.twilio.kudu.sql.KuduMethod;
import com.twilio.kudu.sql.KuduPhysType;
import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.KuduScanStats;
import com.twilio.kudu.sql.KuduWrite;
import com.twilio.kudu.sql.rules.KuduToEnumerableConverter;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator.InputGetter;
import com.twilio.kudu.sql.CalciteKuduTable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.kudu.client.RowResult;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KuduToEnumerableRel extends ConverterImpl implements EnumerableRel {
  public KuduToEnumerableRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new KuduToEnumerableRel(getCluster(), traitSet, sole(inputs));
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
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
    final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));

    // Now build the Java code that represents the Physical scan of a
    // Kudu Table.
    final Expression predicates = list.append("predicates", implementor.stash(kuduImplementor.predicates, List.class));

    final Expression limit = list.append("limit", Expressions.constant(kuduImplementor.limit));

    final Expression offset = list.append("offset", Expressions.constant(kuduImplementor.offset));

    final Expression sorted = list.append("sorted", Expressions.constant(kuduImplementor.sorted));

    final Expression table = list.append("table",
        kuduImplementor.table.getExpression(CalciteKuduTable.KuduQueryable.class));

    final Expression scanStats = list.append("scanStats", implementor.stash(new KuduScanStats(), KuduScanStats.class));

    final Expression cancelBoolean = list.append("cancelBoolean",
        Expressions.convert_(Expressions.call(DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET.method,
            Expressions.constant(DataContext.Variable.CANCEL_FLAG.camelName)), AtomicBoolean.class));

    // @TODO: for correlation variables, for $batchSize,
    // acquire the InputGetter("$cor" + i). Then for all fields, call
    // the InputGetter.field() for that j.
    // and turn those into CalciteKuduPredicates.

    final List<Integer> kuduColumnIndices;
    // If the Query isn't a SELECT *, combine projection and the required in memory
    // Filter columns
    if (!kuduImplementor.kuduProjectedColumns.isEmpty()) {
      kuduColumnIndices = new ArrayList<>(kuduImplementor.kuduProjectedColumns);
      kuduImplementor.filterProjections.stream().forEach(indx -> {
        if (!kuduColumnIndices.contains(indx)) {
          kuduColumnIndices.add(indx);
        }
      });
    }
    // If the query is SELECT *
    else {
      kuduColumnIndices = IntStream.range(0, kuduImplementor.kuduTable.getSchema().getColumnCount()).boxed()
          .collect(Collectors.toList());
    }

    final BlockBuilder projectExpressionBlock = new BlockBuilder();

    final PhysType tablePhystype = new KuduPhysType(kuduImplementor.kuduTable.getSchema(),
        kuduImplementor.tableDataType, kuduImplementor.descendingColumns, kuduColumnIndices);
    final ParameterExpression inputRow = Expressions.parameter(Modifier.FINAL, Object.class, "abstractRow");

    // If we have selected columns add them to the RexProgram as such.
    final List<Pair<RexNode, String>> namedProjects;
    if (!kuduImplementor.projections.isEmpty()) {
      namedProjects = Pair.zip(kuduImplementor.projections, getRowType().getFieldNames());
    } else {
      // Create a Projection that includes every column in the table schema.
      namedProjects = kuduColumnIndices.stream().map(indx -> {
        final RelDataTypeField field = kuduImplementor.table.getRowType().getFieldList().get(indx);
        final RexNode ref = new RexLocalRef(indx, field.getType());
        return Pair.of(ref, field.getName());
      }).collect(Collectors.toList());
    }
    final RexProgramBuilder builder = new RexProgramBuilder(tablePhystype.getRowType(), getCluster().getRexBuilder());

    // Adds all the references we will be using. This might be unnecessary as
    // addProject and
    // addCondition might do this for us.
    kuduColumnIndices.stream()
        .map(indx -> new RexLocalRef(indx, kuduImplementor.table.getRowType().getFieldList().get(indx).getType()))
        .forEach(localRef -> builder.addExpr(localRef));

    namedProjects.stream().forEach(pair -> builder.addProject(pair.left, pair.right));

    if (kuduImplementor.inMemoryCondition != null) {
      builder.addCondition(kuduImplementor.inMemoryCondition);
    }

    final Expression castToRow = Expressions.convert_(inputRow, RowResult.class);

    final InputGetter inputGetter = new RexToLixTranslator.InputGetterImpl(
        Collections.singletonList(Pair.of(castToRow, tablePhystype)));
    final RexProgram projectionFunctions = builder.getProgram();
    final List<Expression> projectionExpressions = RexToLixTranslator.translateProjects(projectionFunctions,
        implementor.getTypeFactory(), implementor.getConformance(), projectExpressionBlock, tablePhystype,
        DataContext.ROOT, inputGetter, null);

    projectExpressionBlock.add(Expressions.return_(null, physType.record(projectionExpressions)));

    // This is the map function that will always be present. It translates the
    // RowResult into an
    // Object[]
    final Expression mapFunction = Expressions.new_(Function1.class, Collections.emptyList(), Expressions.methodDecl(
        Modifier.PUBLIC, Object.class, "apply", Collections.singletonList(inputRow), projectExpressionBlock.toBlock()));

    final Expression filterFunction;
    if (!kuduImplementor.filterProjections.isEmpty()) {
      // This is builds a predicate function that will always be present. It checks if
      // the RowResult
      // should be returned in the KuduEnumerable.
      final BlockBuilder filterBuilder = new BlockBuilder();
      final Expression condition = RexToLixTranslator.translateCondition(projectionFunctions,
          implementor.getTypeFactory(), filterBuilder, inputGetter, null, implementor.getConformance());
      filterBuilder.add(Expressions.return_(null, condition));
      filterFunction = Expressions.new_(Predicate1.class, Collections.emptyList(), Expressions.methodDecl(
          Modifier.PUBLIC, boolean.class, "apply", Collections.singletonList(inputRow), filterBuilder.toBlock()));

    } else {
      try {
        filterFunction = Expressions.field(null, Predicate1.class.getDeclaredField("TRUE"));
      } catch (NoSuchFieldException | SecurityException e) {
        throw new RuntimeException("Unable to access the TRUE predicate from Predicate1 class", e);
      }
    }

    final Expression fields = list.append("kuduFields", implementor.stash(kuduColumnIndices, List.class));

    // If the output type has a Single column, inform the ScannerCallback.
    final Expression isSingleObject = Expressions.constant(physType.getRowType().getFieldCount() == 1);

    List<Integer> sortedPkColumnIndexes = getPrimaryKeyColumnsInProjection(kuduImplementor.sortPkColumns,
        kuduColumnIndices);
    final Expression sortedPrefixKeySelector;
    if (kuduImplementor.groupByLimited) {
      sortedPrefixKeySelector = physType.generateAccessor(sortedPkColumnIndexes);
    } else {
      sortedPrefixKeySelector = Expressions.constant(null, Function1.class);
    }
    Expressions.list(list.append("keySelector", sortedPrefixKeySelector));

    final Expression sortPkColumns = list.append("sortPkColumns", implementor.stash(sortedPkColumnIndexes, List.class));

    boolean groupBySortedOrLimited = kuduImplementor.groupBySorted || kuduImplementor.groupByLimited;
    final Expression enumerable = list.append("enumerable",
        Expressions.call(table, KuduMethod.KUDU_QUERY_METHOD.method, predicates, fields, limit, offset, sorted,
            Expressions.constant(groupBySortedOrLimited), scanStats, cancelBoolean, mapFunction, filterFunction,
            isSingleObject, sortedPrefixKeySelector, sortPkColumns));

    Hook.QUERY_PLAN.run(predicates);
    list.add(Expressions.return_(null, enumerable));

    KuduToEnumerableConverter.logger.debug("Created a KuduQueryable " + list.toBlock());
    return implementor.result(physType, list.toBlock());
  }

  /**
   * Return the Integer indices in the Row Projection that match the primary key
   * columns and in the order they need to match. This lays out how to compare two
   * {@code CalciteRow}s and determine which one is smaller.
   * <p>
   * As an example, imagine we have a table (A, B, C, D, E) with primary columns
   * in order of (A, B) and we have a scanner SELECT D, C, E, B, A the
   * projectedSchema will be D, C, E, B, A and the tableSchema will be A, B, C, D,
   * E *this* function will return List(4, 3) -- the position's of A and B within
   * the projection and in the order they need to be sorted by.
   * <p>
   * The returned index list is used by the sorted {@link KuduEnumerable} to merge
   * the results from multiple scanners.
   *
   * @param projectedColumnIndices the indices of the columns that are being
   *                               selected or required to evaluate a filter in
   *                               memory
   *
   * @param sortPkColumnIndices    the indices of the primary key columns that are
   *                               present in the ORDER BY clause
   *
   * @return List of column indexes that part of the primary key in the Kudu
   *         Sorted order
   */
  public List<Integer> getPrimaryKeyColumnsInProjection(final List<Integer> sortPkColumnIndices,
      final List<Integer> projectedColumnIndices) {
    final List<Integer> primaryKeyColumnsInProjection = new ArrayList<>();
    // KuduSortRule checks if the prefix of the primary key columns are being
    // filtered and are
    // set to a constant literal, or if the columns being sorted are a prefix of the
    // primary key
    // columns.
    for (int sortPkColumnIndex : sortPkColumnIndices) {
      boolean found = false;
      for (int i = 0; i < projectedColumnIndices.size(); ++i) {
        int projectedColumnIndex = projectedColumnIndices.get(i);
        if (sortPkColumnIndex == projectedColumnIndex) {
          primaryKeyColumnsInProjection.add(i);
          found = true;
        }
      }
      if (!found) {
        String projectedColumnIndicesString = projectedColumnIndices.stream().map(Object::toString)
            .collect(Collectors.joining(", "));
        String sortPkColumnIndicesString = sortPkColumnIndices.stream().map(Object::toString)
            .collect(Collectors.joining(", "));
        throw new IllegalStateException("Unable to find primary key column index " + sortPkColumnIndex
            + " in the projection, " + "projectedColumnIndices " + projectedColumnIndicesString
            + " sortPkColumnIndices " + sortPkColumnIndicesString);
      }
    }
    return primaryKeyColumnsInProjection;
  }

  private Result executeMutation(EnumerableRelImplementor implementor, Prefer prefer) {
    // Generates a call to "mutate" with the appropriate parameters
    final BlockBuilder list = new BlockBuilder();
    final RelDataType rowType = getRowType();
    final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, prefer.preferArray());

    final KuduRelNode.Implementor kuduImplementor = new KuduRelNode.Implementor();
    // This goes and visits the entire tree, setting up kuduImplementor
    // with the columnNames and tuples
    kuduImplementor.visitChild(0, getInput());

    // Now build the Java code to execute the mutation
    final Expression columnNames = list.append("columnIndexes",
        implementor.stash(kuduImplementor.columnIndexes, List.class));

    final Expression table = list.append("table",
        kuduImplementor.table.getExpression(CalciteKuduTable.KuduQueryable.class));

    if (kuduImplementor.numBindExpressions != 0) {
      // a PreparedStatement was used
      final Expression values = list.append("values", valuesArrayList(list, kuduImplementor.numBindExpressions));
      Expression enumerable = list.append("enumerable",
          Expressions.call(table, KuduMethod.KUDU_MUTATE_ROW_METHOD.method, columnNames, values));
      list.add(Expressions.return_(null, enumerable));
    } else {
      // a regular Statement was used
      final Expression tuples = list.append("tuples", implementor.stash(kuduImplementor.tuples, List.class));
      Expression enumerable = list.append("enumerable",
          Expressions.call(table, KuduMethod.KUDU_MUTATE_TUPLES_METHOD.method, columnNames, tuples));
      list.add(Expressions.return_(null, enumerable));
    }

    KuduToEnumerableConverter.logger.debug("Created a KuduMutation " + list.toBlock());
    return implementor.result(physType, list.toBlock());
  }

  private static MethodCallExpression valuesArrayList(final BlockBuilder list, final int numBindExpressions) {
    return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method,
        Expressions.newArrayInit(Object.class, bindExpressions(list, numBindExpressions)));
  }

  private static List<Expression> bindExpressions(final BlockBuilder list, final int numBindExpressions) {
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
