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
    boolean projectExpressions = false;
    for (RexNode e : project.getProjects()) {
      if (!(e instanceof RexInputRef)) {
        projectExpressions = true;
        break;
      }
    }
    // if the Projection includes expressions then we split the Projection into a
    // KuduProjectRel that includes only column values which is wrapped by a
    // LogicalCalc that computes the expressions
    if (projectExpressions) {
      KuduProjectRel.KuduProjectTransformer transformer =
          new KuduProjectRel.KuduProjectTransformer();
      // we need to transform the original projection to reference the output from the
      // KuduProjectRel that is being created
      final List<RexNode> transformedProjects =
          project.getProjects()
              .stream()
              .map(rexNode -> rexNode.accept(transformer))
              .collect(Collectors.toList());
      LinkedHashMap<Integer, RelDataTypeField> projectedColumnToRelDataTypeFieldMap =
          transformer.getProjectedColumnToRelDataTypeFieldMap();
      final List<RexNode> kuduProjects =
          projectedColumnToRelDataTypeFieldMap.entrySet()
              .stream()
              .map(entry ->
                  new RexInputRef(entry.getKey(), entry.getValue().getType()))
              .collect(Collectors.toList());
      final RelRecordType relRecordType = new RelRecordType(
          projectedColumnToRelDataTypeFieldMap.entrySet()
              .stream()
              .map(entry -> entry.getValue())
              .collect(Collectors.toList())
      );

      // create a KuduProjectRel that contains the required columns
      final KuduProjectRel kuduProjection = new KuduProjectRel(project.getCluster(),
          project.getCluster().traitSetOf(KuduRelNode.CONVENTION), convert(project.getInput(), KuduRelNode.CONVENTION), kuduProjects,
          relRecordType, projectExpressions);

      // wrap the KuduProjectRel in a LogicalCalc
      final RexProgram program =
        RexProgram.create(
          kuduProjection.getRowType(),
          transformedProjects,
          null,
          project.getRowType(),
          project.getCluster().getRexBuilder());
      final LogicalCalc calc = new LogicalCalc(kuduProjection.getCluster(), project.getTraitSet(), ImmutableList.of(), kuduProjection, program);
      call.transformTo(calc);
    } else {
      final RelTraitSet traitSet = project.getTraitSet().replace(KuduRelNode.CONVENTION);
      // just replace the LogicalProject with a KuduProjectRel
      final RelNode newProjection = new KuduProjectRel(project.getCluster(),
          traitSet,
          convert(project.getInput(), KuduRelNode.CONVENTION),
          project.getProjects(),
          project.getRowType(),
          projectExpressions);
      call.transformTo(newProjection);
    }
  }

}
