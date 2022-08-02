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
package com.twilio.kudu.sql.rel;

import com.twilio.kudu.sql.KuduRelNode;
import com.twilio.kudu.sql.metadata.KuduRelMetadataProvider;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class KuduLimitRel extends SingleRel implements KuduRelNode {
  public final RexNode offset;
  public final RexNode fetch;

  public KuduLimitRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, input);
    this.offset = offset;
    this.fetch = fetch;
    assert getConvention() == input.getConvention();
    // include our own metadata provider so that we can customize costs
    JaninoRelMetadataProvider relMetadataProvider = JaninoRelMetadataProvider.of(KuduRelMetadataProvider.INSTANCE);
    RelMetadataQuery.THREAD_PROVIDERS.set(relMetadataProvider);
    getCluster().setMetadataProvider(relMetadataProvider);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeTinyCost();
  }

  @Override
  public KuduLimitRel copy(RelTraitSet traitSet, List<RelNode> newInputs) {
    return new KuduLimitRel(getCluster(), traitSet, sole(newInputs), offset, fetch);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    if (offset != null) {
      implementor.offset = RexLiteral.intValue(offset);
    }
    if (fetch != null) {
      implementor.limit = RexLiteral.intValue(fetch);
    }
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    pw.itemIf("offset", offset, offset != null);
    pw.itemIf("limit", fetch, fetch != null);
    return pw;
  }
}
