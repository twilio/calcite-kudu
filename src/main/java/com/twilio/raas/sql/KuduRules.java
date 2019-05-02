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

public class KuduRules {
    protected static final Logger LOGGER = CalciteTrace.getPlannerTracer();


    public static final KuduFilterRule FILTER = new KuduFilterRule(RelFactories.LOGICAL_BUILDER);
    public static final KuduProjectRule PROJECT = new KuduProjectRule(RelFactories.LOGICAL_BUILDER);
    public static List<RelOptRule> RULES = Arrays.asList(FILTER, PROJECT);

  
    public static class KuduProjectRule extends RelOptRule {
        public KuduProjectRule(RelBuilderFactory relBuilderFactory) {
            super(convertOperand(LogicalProject.class, (java.util.function.Predicate<RelNode>)r -> true, Convention.NONE),
                  RelFactories.LOGICAL_BUILDER,
                  "KuduProjection");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
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
        @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
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
}
