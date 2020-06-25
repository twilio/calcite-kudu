package com.twilio.raas.sql.rules;

import com.google.common.collect.ImmutableList;
import com.twilio.raas.sql.KuduRelNode;
import com.twilio.raas.sql.rel.KuduProjectRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class KuduProjectRule extends RelOptRule {

    public KuduProjectRule(RelBuilderFactory relBuilderFactory) {
        super(convertOperand(LogicalProject.class,
                (java.util.function.Predicate<RelNode>) r -> true, Convention.NONE),
            relBuilderFactory, "KuduProjection");
    }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = (LogicalProject) call.getRelList().get(0);
    final RelTraitSet traitSet = project.getTraitSet().replace(KuduRelNode.CONVENTION);
    // just replace the LogicalProject with a KuduProjectRel
    final RelNode newProjection = new KuduProjectRel(project.getCluster(),
        traitSet,
        convert(project.getInput(), KuduRelNode.CONVENTION),
        project.getProjects(),
        project.getRowType());
    call.transformTo(newProjection);
  }
}
