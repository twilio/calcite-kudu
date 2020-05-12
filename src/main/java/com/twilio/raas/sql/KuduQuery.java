package com.twilio.raas.sql;

import com.twilio.raas.sql.rules.KuduProjectMergeRule;
import com.twilio.raas.sql.rules.KuduRules;
import com.twilio.raas.sql.rules.KuduToEnumerableConverter;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kudu.client.KuduTable;

import java.util.List;

/**
 * Relational expression representing a scan of a KuduTable
 */
public final class KuduQuery extends TableScan implements KuduRelNode {
    final public KuduTable openedTable;

   /**
    * List of column indices that are stored in reverse order.
    */
    final public List<Integer> descendingSortedFieldIndices;
    final public RelDataType projectRowType;

    /**
     *
     * @param cluster        Cluster
     * @param traitSet       Traits
     * @param table          Table
     * @param openedTable    Kudu table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    public KuduQuery(RelOptCluster cluster,
                     RelTraitSet traitSet,
                     RelOptTable table,
                     KuduTable openedTable,
                     List<Integer> descendingSortedFieldIndices,
                     RelDataType projectRowType) {
        super(cluster, traitSet, table);
        this.openedTable = openedTable;
        this.descendingSortedFieldIndices = descendingSortedFieldIndices;
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
        // since kudu is a columnar store we never want to push an aggregate past a project
        planner.removeRule(AggregateProjectMergeRule.INSTANCE);
        // see WirelessUsageIt.testPaginationOverFactAggregation for why this rule is removed
        // disable ReduceExpressionsRule until RC-1274 is implemented
        planner.removeRule(ReduceExpressionsRule.FILTER_INSTANCE);
        // ProjectMergeRule is replaced by KuduProjectMergeRule which does not merge a LogicalCalc
        // that wraps a LogicalProject that is created by KuduProjectRule to handle expressions
        // being projected
        planner.removeRule(ProjectMergeRule.INSTANCE);
        for (RelOptRule rule : KuduRules.RULES) {
          planner.addRule(rule);
        }
    }

    @Override
    public void implement(Implementor impl) {
        // Doesn't call visit child as it is the leaf.
        impl.kuduTable = this.openedTable;
        impl.table = this.table;
    }
}
