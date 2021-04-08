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

import com.google.common.collect.ImmutableList;
import com.twilio.kudu.sql.rules.KuduRules;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Relational expression representing a scan of a KuduTable
 */
public final class KuduQuery extends TableScan implements KuduRelNode {
  final public CalciteKuduTable calciteKuduTable;

  /**
   * List of column indices that are stored in reverse order.
   */
  final public RelDataType projectRowType;

  /**
   * @param cluster          Cluster
   * @param traitSet         Traits
   * @param table            Table
   * @param calciteKuduTable Kudu table
   * @param projectRowType   Fields and types to project; null to project raw row
   */
  public KuduQuery(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CalciteKuduTable calciteKuduTable,
      RelDataType projectRowType) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.calciteKuduTable = calciteKuduTable;
    this.projectRowType = projectRowType;
    assert getConvention() == KuduRelNode.CONVENTION;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override
  public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override
  public void register(RelOptPlanner planner) {
    // since kudu is a columnar store we never want to push an aggregate past a
    // project
    planner.removeRule(CoreRules.AGGREGATE_PROJECT_MERGE);

    // Join Commute Rule reorders the left and right tables of inner joins
    // preventing KuduSortRel
    // from being applied to the large table.
    // {@link
    // org.apache.calcite.rel.metadata.RelMdCollation#enumerableJoin0(org.apache.calcite.rel.metadata.RelMetadataQuery)}
    planner.removeRule(CoreRules.JOIN_COMMUTE);
    planner.removeRule(CoreRules.JOIN_COMMUTE_OUTER);

    // After removing the Join Commute Rule, Merge Join is chosen over Hash Join,
    // negating the work down to push the KuduSortRel into the large table.
    planner.removeRule(org.apache.calcite.adapter.enumerable.EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);

    // This rule does not handle SArg filters correctly, which causes filters to not
    // get pushed
    // past outer joins, which causes KuduSortRule to not get matched. Replace it
    // with
    // KuduFilterIntoJoinRule which expands SArgs
    planner.removeRule(CoreRules.FILTER_INTO_JOIN);

    for (RelOptRule rule : KuduRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override
  public void implement(Implementor impl) {
    // Doesn't call visit child as it is the leaf.
    impl.kuduTable = this.calciteKuduTable.getKuduTable();
    impl.descendingColumns = this.calciteKuduTable.getDescendingOrderedColumnIndexes();
    impl.table = this.table;
    impl.tableDataType = getRowType();
  }
}
