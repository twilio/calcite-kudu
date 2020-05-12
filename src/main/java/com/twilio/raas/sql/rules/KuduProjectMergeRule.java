package com.twilio.raas.sql.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Merges two projections unless they were created by {@link KuduProjectRule}
 */
public class KuduProjectMergeRule extends ProjectMergeRule {

  public static final KuduProjectMergeRule INSTANCE =
    new KuduProjectMergeRule(true, DEFAULT_BLOAT, RelFactories.LOGICAL_BUILDER);

  public KuduProjectMergeRule(boolean force, int bloat, RelBuilderFactory relBuilderFactory) {
    super(force, bloat, relBuilderFactory);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Project topProject = call.rel(0);
    // if the topProject includes an expression don't merge it with the bottom project
    for (RexNode e : topProject.getProjects()) {
      if (!(e instanceof RexInputRef)) {
        return false;
      }
    }
    final Project bottomProject = call.rel(1);
    return topProject.getConvention() == bottomProject.getConvention();
  }

}
