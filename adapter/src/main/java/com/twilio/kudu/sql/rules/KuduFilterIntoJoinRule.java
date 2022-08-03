/* Copyright 2021 Twilio, Inc
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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

public class KuduFilterIntoJoinRule extends FilterJoinRule.FilterIntoJoinRule {

  public static final KuduFilterIntoJoinRule KUDU_FILTER_INTO_JOIN = new KuduFilterIntoJoinRule(
      FilterIntoJoinRuleConfig.DEFAULT);

  protected KuduFilterIntoJoinRule(FilterIntoJoinRuleConfig config) {
    super(config);
  }

  protected void perform(RelOptRuleCall call, Filter filter, Join join) {
    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
    // remove Sargs
    final RexNode condition = RexUtil.flatten(rexBuilder,
        RexUtil.expandSearch(rexBuilder, null, filter.getCondition()));
    final Filter transformedFilter = filter.copy(filter.getTraitSet(), filter.getInput(), condition);
    super.perform(call, transformedFilter, join);
  }

}
