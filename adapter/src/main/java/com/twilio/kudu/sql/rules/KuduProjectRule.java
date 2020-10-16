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
package com.twilio.kudu.sql.rules;

import com.google.common.collect.ImmutableList;
import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.rel.KuduProjectRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.tools.RelBuilderFactory;

public class KuduProjectRule extends RelOptRule {

  public KuduProjectRule(RelBuilderFactory relBuilderFactory) {
    super(convertOperand(LogicalProject.class, (java.util.function.Predicate<RelNode>) r -> true, Convention.NONE),
        relBuilderFactory, "KuduProjection");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = (LogicalProject) call.getRelList().get(0);
    final RelTraitSet traitSet = project.getTraitSet().replace(KuduRelNode.CONVENTION);
    // just replace the LogicalProject with a KuduProjectRel
    final RelNode newProjection = new KuduProjectRel(project.getCluster(), traitSet,
        convert(project.getInput(), KuduRelNode.CONVENTION), project.getProjects(), project.getRowType());
    call.transformTo(newProjection);
  }
}
