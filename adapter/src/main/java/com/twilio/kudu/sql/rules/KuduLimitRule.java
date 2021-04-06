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

import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.rel.KuduLimitRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;

public class KuduLimitRule extends RelOptRule {

  public KuduLimitRule() {
    super(operand(Sort.class, any()), "KuduLimitRule");
  }

  public RelNode convert(Sort limit) {
    final RelTraitSet traitSet = limit.getTraitSet().replace(KuduRelNode.CONVENTION);
    return new KuduLimitRel(limit.getCluster(), traitSet, convert(limit.getInput(), KuduRelNode.CONVENTION),
        limit.offset, limit.fetch);
  }

  public void onMatch(RelOptRuleCall call) {
    final Sort limit = call.rel(0);
    if (limit.getCollation() == RelCollations.EMPTY && (limit.fetch != null || limit.offset != null)) {
      final RelNode converted = convert(limit);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }

}
