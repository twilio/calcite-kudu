/* Copyright 2022 Twilio, Inc
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
package org.apache.calcite.rel.rules.materialize;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

public class KuduMaterializedViewOnlyAggregateRule
    extends KuduMaterializedViewAggregateRule<MaterializedViewOnlyAggregateRule.Config> {

  public KuduMaterializedViewOnlyAggregateRule(MaterializedViewOnlyAggregateRule.Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public KuduMaterializedViewOnlyAggregateRule(RelBuilderFactory relBuilderFactory, boolean generateUnionRewriting,
      HepProgram unionRewritingPullProgram) {
    this(MaterializedViewOnlyAggregateRule.Config.create(relBuilderFactory)
        .withGenerateUnionRewriting(generateUnionRewriting).withUnionRewritingPullProgram(unionRewritingPullProgram)
        .as(MaterializedViewOnlyAggregateRule.Config.class));
  }

  @Deprecated // to be removed before 2.0
  public KuduMaterializedViewOnlyAggregateRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory,
      String description, boolean generateUnionRewriting, HepProgram unionRewritingPullProgram,
      RelOptRule filterProjectTransposeRule, RelOptRule filterAggregateTransposeRule,
      RelOptRule aggregateProjectPullUpConstantsRule, RelOptRule projectMergeRule) {
    this(MaterializedViewOnlyAggregateRule.Config.create(relBuilderFactory)
        .withGenerateUnionRewriting(generateUnionRewriting).withUnionRewritingPullProgram(unionRewritingPullProgram)
        .withDescription(description).withOperandSupplier(b -> b.exactly(operand))
        .as(MaterializedViewOnlyAggregateRule.Config.class).withFilterProjectTransposeRule(filterProjectTransposeRule)
        .withFilterAggregateTransposeRule(filterAggregateTransposeRule)
        .withAggregateProjectPullUpConstantsRule(aggregateProjectPullUpConstantsRule)
        .withProjectMergeRule(projectMergeRule).as(MaterializedViewOnlyAggregateRule.Config.class));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    perform(call, null, aggregate);
  }

}
