package com.twilio.raas.sql;

import org.slf4j.Logger;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.rel.core.Filter;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.Convention;
import org.apache.kudu.client.KuduPredicate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import java.util.Arrays;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import com.twilio.raas.sql.KuduRel.Implementor;
import java.util.stream.Collectors;
import org.apache.calcite.util.Pair;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.kudu.client.KuduTable;
import org.apache.calcite.rex.RexSlot;
import com.google.common.collect.ImmutableList;
import com.twilio.raas.sql.KuduRules.KuduFieldVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import java.util.function.Predicate;
import com.twilio.raas.sql.KuduToEnumerableConverter.KuduToEnumerableRel;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.adapter.enumerable.EnumerableRel.Result;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableRel.Prefer;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;

public class KuduRules {
    protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();


    public static final KuduFilterRule FILTER = new KuduFilterRule(RelFactories.LOGICAL_BUILDER);
    public static final KuduProjectRule PROJECT = new KuduProjectRule(RelFactories.LOGICAL_BUILDER);
    public static final KuduSortWithFilterRule FILTER_SORT = new KuduSortWithFilterRule(RelFactories.LOGICAL_BUILDER);
    public static final KuduSortWithoutFilterRule SORT = new KuduSortWithoutFilterRule(RelFactories.LOGICAL_BUILDER);
    public static List<RelOptRule> RULES = Arrays.asList(
        FILTER,
        PROJECT,
        SORT,
        FILTER_SORT);

    /**
     * When a query is executed without a filter, check to see if the sort is in
     * Kudu Primary Key order. If so, tell Enumerable to sort results
     */
    public static class KuduSortWithoutFilterRule extends RelOptRule {
        KuduSortWithoutFilterRule(RelBuilderFactory relBuilderFactory) {
            super(operand(
                    Sort.class, operand(KuduToEnumerableRel.class,
                        some(operand(KuduQuery.class, none())))),
                    relBuilderFactory, "KuduSimpleSort");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final KuduQuery query = (KuduQuery)call.getRelList().get(2);
            final KuduTable openedTable = query.openedTable;
            final Sort originalSort = (Sort)call.getRelList().get(0);

            // If there is no sort -- i.e. there is only a limit
            // don't pay the cost of returning rows in sorted order.
            if (originalSort.getCollation().getFieldCollations().isEmpty()) {
                return;
            }

            int mustMatch = 0;

            for (RelFieldCollation sortField: originalSort.getCollation().getFieldCollations()) {
                if (sortField.direction != RelFieldCollation.Direction.ASCENDING &&
                    sortField.direction != RelFieldCollation.Direction.STRICTLY_ASCENDING) {
                    return;
                }
                if (sortField.getFieldIndex() >= openedTable.getSchema().getPrimaryKeyColumnCount() ||
                    sortField.getFieldIndex() != mustMatch) {
                    // This field is not in the primary key columns. Can't sort this.
                    return;
                }
                mustMatch++;
            }

            // Now transform call into our new rule. This new rule will generate
            // EnumerableRel.Result -- which contains the java code to compile.
            final RelNode input = originalSort.getInput();
            final RelNode newNode = KuduSortRel.create(
                convert(
                    input,
                    input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
                originalSort.getCollation(),
                originalSort.offset,
                originalSort.fetch);
            call.transformTo(newNode);
        }
    }

    /**
     * When a sort includes one or more of the primary keys  in the sorted
     * direction but the other primary key is in an equivalence filter, our
     * Enumerable can still return the result that respects the sort.
     *
     * For instance, a query that filters on account_id -- which is the first
     * key -- and sorts by date_created field which is the second (and a range)
     * key, the enumerable can return the results in sorted order.
     */
    public static class KuduSortWithFilterRule extends RelOptRule {
        public KuduSortWithFilterRule (RelBuilderFactory relBuilderFactory) {
            super(operand(Sort.class, operand(KuduToEnumerableRel.class, some(
                            operand(KuduFilterRel.class, some(operand(KuduQuery.class, none())))))),
                relBuilderFactory, "KuduFilterSort");
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final KuduQuery query = (KuduQuery)call.getRelList().get(3);
            final KuduFilterRel filter = (KuduFilterRel)call.getRelList().get(2);
            final KuduTable openedTable = query.openedTable;
            final Sort originalSort = (Sort)call.getRelList().get(0);
            int mustMatch = 0;
            RexNode originalCondition = filter.getCondition();

            // If there is no sort -- i.e. there is only a limit
            // don't pay the cost of returning rows in sorted order.
            if (originalSort.getCollation().getFieldCollations().isEmpty()) {
                return;
            }

            for (RelFieldCollation sortField: originalSort.getCollation().getFieldCollations()) {
                if (sortField.direction != RelFieldCollation.Direction.ASCENDING &&
                    sortField.direction != RelFieldCollation.Direction.STRICTLY_ASCENDING) {
                    return;
                }
                if (sortField.getFieldIndex() >= openedTable.getSchema().getPrimaryKeyColumnCount()) {
                    // This field is not in the primary key columns. Can't sort this.
                    return;
                }
                if (sortField.getFieldIndex() != mustMatch) {
                    // Go look at the condition to see if we have an exact match on the condition.
                    while (mustMatch < sortField.getFieldIndex()) {
                        final KuduFilterVisitor visitor = new KuduFilterVisitor(mustMatch);
                        final Boolean foundFieldInCondition = originalCondition.accept(visitor);
                        if (foundFieldInCondition == Boolean.FALSE) {
                            return;
                        }
                        mustMatch++;
                    }
                }
                mustMatch++;
            }

            // Now transform call into our new rule. This new rule will generate
            // EnumerableRel.Result -- which contains the java code to compile.
            final RelNode input = originalSort.getInput();
            final RelNode newNode = KuduSortRel.create(
                convert(
                    input,
                    input.getTraitSet().replace(EnumerableConvention.INSTANCE)),
                originalSort.getCollation(),
                originalSort.offset,
                originalSort.fetch);
            call.transformTo(newNode);
        }
    }

    /**
     * This relation takes as input the {@link KuduToEnumerableRel} and
     * wraps it's output from {@link KuduToEnumerableRel#implement} into another
     * code block that calls {@link SortableEnumerable#setSorted} and optionally
     * calls {@link SortableEnumerable#setLimit} and
     * {@link SortableEnumerable#setOffset}.
     */
    public static class KuduSortRel extends Sort implements EnumerableRel {
        public KuduSortRel(RelOptCluster cluster, RelTraitSet traitSet,
            RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
            super(cluster, traitSet, input, collation, offset, fetch);
            assert getConvention() instanceof EnumerableConvention;
            assert getConvention() == input.getConvention();
        }

        public static KuduSortRel create(RelNode child, RelCollation collation,
            RexNode offset, RexNode fetch) {
            final RelOptCluster cluster = child.getCluster();
            final RelTraitSet traitSet =
                cluster.traitSetOf(EnumerableConvention.INSTANCE)
                .replace(collation);
            return new KuduSortRel(cluster, traitSet, child, collation, offset,
                fetch);
        }

        @Override
        public KuduSortRel copy(
            RelTraitSet traitSet,
            RelNode newInput,
            RelCollation newCollation,
            RexNode offset,
            RexNode fetch) {
            return new KuduSortRel(getCluster(), traitSet, newInput, newCollation,
                offset, fetch);
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
            if (fetch != null) {
                return super.computeSelfCost(planner, mq).multiplyBy(0.01);
            }
            return super.computeSelfCost(planner, mq).multiplyBy(0.02);
        }

        @Override
        public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
            BlockBuilder builder = new BlockBuilder();
            final EnumerableRel child = (EnumerableRel) getInput();
            final Result result = implementor.visitChild(this, 0, child, pref);
            final PhysType physType =
                PhysTypeImpl.of(
                    implementor.getTypeFactory(),
                    getRowType(),
                    result.format);

            PhysType inputPhysType = result.physType;

            Expression storedScan = builder.append("storedScan",
                Expressions.convert_(
                    builder.append("scan", result.block),
                    SortableEnumerable.class));

            if (offset != null ) {
                final RexLiteral parsedOffset = (RexLiteral) offset;
                final Long properOffset = (Long)parsedOffset.getValue2();
                builder.add(
                    Expressions.statement(Expressions.call(storedScan,
                            "setOffset",
                            Expressions.constant(properOffset))));
            }

            if (fetch != null) {
                final RexLiteral parsedFetch = (RexLiteral) fetch;
                final Long properFetch = (Long)parsedFetch.getValue2();

                builder.add(
                    Expressions.statement(Expressions.call(storedScan,
                        "setLimit",
                            Expressions.constant(properFetch))));
            }

            builder.add(Expressions.call(storedScan, "setSorted"));

            System.out.println(builder.toBlock());
            return implementor.result(physType, builder.toBlock());
        }
    }

    public static class KuduProjectRule extends RelOptRule {
        public KuduProjectRule(RelBuilderFactory relBuilderFactory) {
            super(convertOperand(LogicalProject.class, (java.util.function.Predicate<RelNode>)r -> true, Convention.NONE),
                  RelFactories.LOGICAL_BUILDER,
                  "KuduProjection");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            // @TODO: this rule doesn't get applied as often as expected.
            LogicalProject project = (LogicalProject) call.getRelList().get(0);
            for (RexNode e : project.getProjects()) {
                if (!(e instanceof RexInputRef)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalProject project = (LogicalProject) call.getRelList().get(0);
            final RelTraitSet traitSet = project.getTraitSet().replace(KuduRel.CONVENTION);
            if (project.getTraitSet().contains(Convention.NONE)) {
                final RelNode newProjection = new KuduProjectRel(project.getCluster(),
                                                                 traitSet,
                                                                 convert(project.getInput(), KuduRel.CONVENTION),
                                                                 project.getProjects(),
                                                                 project.getRowType());
                call.transformTo(newProjection);
            }
        }
    }

    public static class KuduProjectRel extends Project implements KuduRel {
        public KuduProjectRel(RelOptCluster cluster, RelTraitSet traitSet,
                              RelNode input, List<? extends RexNode> projects,
                              RelDataType rowType) {
            super(cluster, traitSet, input, projects, rowType);
        }
        @Override
        public Project copy(RelTraitSet traitSet, RelNode input,
                            List<RexNode> projects, RelDataType rowType) {
            return new KuduProjectRel(getCluster(), traitSet, input, projects,
                                      rowType);
        }
        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner,
                                                    RelMetadataQuery mq) {
            return super.computeSelfCost(planner, mq).multiplyBy(0.1);
        }

        @Override
        public void implement(Implementor implementor) {
            implementor.visitChild(0, getInput());
            final KuduFieldVisitor visitor = new KuduFieldVisitor(getInput().getRowType().getFieldNames());
            implementor.kuduProjectedColumns
                .addAll(getNamedProjects()
                        .stream()
                        .map(calcitePair -> {
                                return calcitePair.left.accept(visitor);
                            })
                        .collect(Collectors.toList()));
        }
    }

    public static class KuduFilterRule extends RelOptRule {
        public KuduFilterRule(RelBuilderFactory relBuilderFactory) {
            super(operand(LogicalFilter.class, operand(KuduQuery.class, none())),
                  relBuilderFactory, "KuduPushDownFilters");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            // This is set to always be called with KuduQuery
            // so no other checks are required.
            return true;
        }

        /**
         * When we match, this method needs to transform the
         * {@link RelOptRuleCall} into a new rule with push
         * filters applied.
         */
        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalFilter filter = (LogicalFilter)call.getRelList().get(0);
            final KuduQuery scan = (KuduQuery)call.getRelList().get(1);
            if (filter.getTraitSet().contains(Convention.NONE)) {
                final KuduPushDownRule predicateParser = new KuduPushDownRule(Collections.emptyList(), scan.openedTable.getSchema());
                List<List<CalciteKuduPredicate>> predicates = filter.getCondition().accept(predicateParser, null);
                if (predicates.isEmpty()) {
                    return;
                }
                final RelNode converted = new KuduFilterRel(filter.getCluster(),
                                                            filter.getTraitSet().replace(KuduRel.CONVENTION),
                                                            convert(filter.getInput(), KuduRel.CONVENTION), // @TODO: what is this call
                                                            filter.getCondition(),
                                                            predicates);
                if (predicateParser.areAllFiltersApplied()) {
                    call.transformTo(converted);
                }
                else {
                    call.transformTo(filter.copy(filter.getTraitSet(),
                                                 ImmutableList.of(converted)));
                }

            }
        }
    }

    public static class KuduFilterRel extends Filter implements KuduRel {
        public final List<List<CalciteKuduPredicate>> predicates;

        public KuduFilterRel(RelOptCluster cluster,
                             RelTraitSet traitSet,
                             RelNode child,
                             RexNode condition,
                             List<List<CalciteKuduPredicate>> predicates) {
            super(cluster, traitSet, child, condition);
            this.predicates = predicates;
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner,
                                          RelMetadataQuery mq) {
            // Really lower the cost.
            // @TODO: consider if the predicates include primary keys
            // and adjust the cost accordingly.
            return super.computeSelfCost(planner, mq).multiplyBy(0.1);
        }

        public KuduFilterRel copy(RelTraitSet traitSet, RelNode input,
                                  RexNode condition) {
            return new KuduFilterRel(getCluster(), traitSet, input, condition,
                                     this.predicates);
        }

        @Override
        public void implement(Implementor implementor) {
            implementor.visitChild(0, getInput());
            implementor.predicates.addAll(this.predicates);
        }
    }

    public static class KuduFieldVisitor extends RexVisitorImpl<Integer> {
        public KuduFieldVisitor(List<String> fieldNamesFromInput) {
            super(true);
        }
        @Override
        public Integer visitInputRef(RexInputRef inputRef) {
            // @TODO: this is so lame.
            return inputRef.getIndex();
        }
    }

    public static class KuduFilterVisitor extends RexVisitorImpl<Boolean> {
        public final int mustHave;
        public KuduFilterVisitor(int mustHave) {
            super(true);
            this.mustHave = mustHave;
        }

        public Boolean visitInputRef(RexInputRef inputRef) {
            //            new Exception().printStackTrace();
            return inputRef.getIndex() == this.mustHave;
        }

        public Boolean visitLocalRef(RexLocalRef localRef) {
            return Boolean.FALSE;
        }

        public Boolean visitLiteral(RexLiteral literal) {
            return Boolean.FALSE;
        }

        public Boolean visitCall(RexCall call) {
            switch (call.getOperator().getKind()) {
            case EQUALS:
                return call.operands.get(0).accept(this);
            case AND:
                for (RexNode operand : call.operands) {
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
