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
package com.twilio.kudu.sql;

import com.twilio.kudu.sql.metadata.KuduRelMetadataProvider;
import com.twilio.kudu.sql.rules.KuduProjectValuesRule;
import com.twilio.kudu.sql.rules.KuduToEnumerableConverter;
import com.twilio.kudu.sql.rules.KuduValuesRule;
import com.twilio.kudu.sql.rules.KuduWriteRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.kudu.client.KuduTable;

import java.util.List;

public class KuduWrite extends TableModify implements KuduRelNode {

  private KuduTable kuduTable;

  public KuduWrite(KuduTable kuduTable, RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader,
      RelNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList,
      boolean flattened) {
    super(cluster, cluster.traitSetOf(KuduRelNode.CONVENTION), table, catalogReader, child, operation, updateColumnList,
        sourceExpressionList, flattened);
    this.kuduTable = kuduTable;
    // include our own metadata provider so that we can customize costs
    JaninoRelMetadataProvider relMetadataProvider = JaninoRelMetadataProvider.of(KuduRelMetadataProvider.INSTANCE);
    RelMetadataQuery.THREAD_PROVIDERS.set(relMetadataProvider);
    getCluster().setMetadataProvider(relMetadataProvider);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    KuduWrite newRel = new KuduWrite(kuduTable, getCluster(), getTable(), getCatalogReader(), sole(inputs),
        getOperation(), getUpdateColumnList(), getSourceExpressionList(), isFlattened());
    newRel.traitSet = traitSet;
    return newRel;
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.clear();
    planner.addRule(KuduProjectValuesRule.INSTANCE);
    planner.addRule(KuduValuesRule.INSTANCE);
    planner.addRule(KuduWriteRule.INSTANCE);
    planner.addRule(KuduToEnumerableConverter.INSTANCE);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.visitChild(0, getInput());
    implementor.table = table;
    implementor.kuduTable = kuduTable;
    implementor.tableDataType = getRowType();
  }

}
