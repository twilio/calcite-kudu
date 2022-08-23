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
import com.twilio.kudu.sql.rules.KuduFilterRule;
import com.twilio.kudu.sql.rules.KuduNestedJoinRule;
import com.twilio.kudu.sql.rules.KuduRules;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Relational expression representing a scan of a KuduTable
 */
public final class KuduQuery extends TableScan implements KuduRelNode {
  final public CalciteKuduTable calciteKuduTable;

  public static HintStrategyTable KUDU_HINT_STRATEGY_TABLE = HintStrategyTable.builder()
      .hintStrategy(KuduNestedJoinRule.HINT_NAME, HintPredicates.JOIN)
      .hintStrategy(KuduFilterRule.HINT_NAME, HintPredicates.TABLE_SCAN).build();

  /**
   * List of column indices that are stored in reverse order.
   */
  final public RelDataType projectRowType;

  /**
   * @param cluster          Cluster
   * @param traitSet         Traits
   * @param table            Table
   * @param hints            {@code RelHint} list
   * @param calciteKuduTable Kudu table
   * @param projectRowType   Fields and types to project; null to project raw row
   */
  public KuduQuery(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, List<RelHint> hints,
      CalciteKuduTable calciteKuduTable, RelDataType projectRowType) {
    super(cluster, traitSet, hints, table);
    this.calciteKuduTable = calciteKuduTable;
    this.projectRowType = projectRowType;
    assert getConvention() == KuduRelNode.CONVENTION;
    // include our own metadata provider so that we can customize costs
    JaninoRelMetadataProvider relMetadataProvider = JaninoRelMetadataProvider.of(KuduRelMetadataProvider.INSTANCE);
    RelMetadataQuery.THREAD_PROVIDERS.set(relMetadataProvider);
    getCluster().setMetadataProvider(relMetadataProvider);
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
    getCluster().setHintStrategies(KUDU_HINT_STRATEGY_TABLE);
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
    planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);

    // This rule does not handle SArg filters correctly, which causes filters to not
    // get pushed
    // past outer joins, which causes KuduSortRule to not get matched. Replace it
    // with
    // KuduFilterIntoJoinRule which expands SArgs
    planner.removeRule(CoreRules.FILTER_INTO_JOIN);

    planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_SORT_RULE);

    KuduRules.CORE_RULES.stream().forEach(rule -> planner.addRule(rule));

    if (CalciteSystemProperty.ENABLE_ENUMERABLE.value()) {
      KuduRules.ENUMERABLE_RULES.stream().forEach(r -> planner.addRule(r));
    }

    // TODO remove if/when CALCITE-5420 is merged
    planner.removeRule(MaterializedViewRules.AGGREGATE);
    planner.addRule(KuduRules.MV_AGGREGATE);

    // we include our own metadata provider that overrides calcite's default Sarg
    // selectivity so
    // that union queries work as expected
    JaninoRelMetadataProvider relMetadataProvider = JaninoRelMetadataProvider.of(KuduRelMetadataProvider.INSTANCE);
    RelMetadataQuery.THREAD_PROVIDERS.set(relMetadataProvider);
  }

  @Override
  public RelNode withHints(List<RelHint> hintList) {
    return new KuduQuery(this.getCluster(), this.traitSet, this.table, hintList, this.calciteKuduTable,
        this.projectRowType);
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
