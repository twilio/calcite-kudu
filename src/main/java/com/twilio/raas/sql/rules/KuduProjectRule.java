package com.twilio.raas.sql.rules;

import com.twilio.raas.sql.KuduRel;
import com.twilio.raas.sql.rel.KuduProjectRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

public class KuduProjectRule extends RelOptRule {

    public KuduProjectRule(RelBuilderFactory relBuilderFactory) {
        super(convertOperand(LogicalProject.class, (java.util.function.Predicate<RelNode>) r -> true, Convention.NONE),
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
