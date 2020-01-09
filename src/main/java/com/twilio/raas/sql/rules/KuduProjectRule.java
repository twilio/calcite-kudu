package com.twilio.raas.sql.rules;

import com.twilio.raas.sql.KuduRel;
import com.twilio.raas.sql.rel.KuduProjectRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class KuduProjectRule extends RelOptRule {

    public KuduProjectRule(RelBuilderFactory relBuilderFactory) {
        super(convertOperand(LogicalProject.class,
            (java.util.function.Predicate<RelNode>) r -> true, Convention.NONE),
            relBuilderFactory, "KuduProjection");
    }

    /**
     * If the LogicalProject wraps a {@link KuduProjectRel} that means this rule was already
     * matched
     * @return false if this rule was already matched, or else true
     */
    public boolean matches(RelOptRuleCall call) {
      for (RelNode relNode : call.getRelList()) {
        if (relNode instanceof Project) {
          RelNode input = ((Project) relNode).getInput();
          if (input instanceof RelSubset) {
            for (RelNode rel : ((RelSubset) input).getRelList()) {
              if (rel instanceof KuduProjectRel) {
                return false;
              }
            }
          }
        }
      }
      return true;
    }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = (LogicalProject) call.getRelList().get(0);
    boolean projectsOnlyColumns = true;
    for (RexNode e : project.getProjects()) {
      if (!(e instanceof RexInputRef)) {
        projectsOnlyColumns = false;
        break;
      }
    }
    final RelTraitSet traitSet = project.getTraitSet().replace(KuduRel.CONVENTION);
    // if the Projection includes expressions then we split the Projection into a
    // KuduProjectRel that includes only column values which is wrapped by a
    // LogicalProject that computes the expressions
    if (!projectsOnlyColumns) {
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
          traitSet, convert(project.getInput(), KuduRel.CONVENTION), kuduProjects,
          relRecordType, false);
      Project wrappedProject = LogicalProject.create(kuduProjection, transformedProjects,
          project.getRowType());
      call.transformTo(wrappedProject);
    } else {
      // just replace the LogicalProject with a KuduProjectRel
      final RelNode newProjection = new KuduProjectRel(project.getCluster(),
          traitSet,
          convert(project.getInput(), KuduRel.CONVENTION),
          project.getProjects(),
          project.getRowType(), true);
      call.transformTo(newProjection);
    }
  }

}
