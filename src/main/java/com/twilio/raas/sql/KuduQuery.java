package com.twilio.raas.sql;

import com.twilio.raas.sql.rules.KuduRules;
import com.twilio.raas.sql.rules.KuduToEnumerableConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.kudu.client.KuduTable;

import java.util.List;

/**
 * Relational expression representing a scan of a KuduTable
 */
public final class KuduQuery extends TableScan implements KuduRel {
    final public KuduTable openedTable;
    final public RelDataType projectRowType;

    /**
     *
     * @param cluster        Cluster
     * @param traitSet       Traits
     * @param table          Table
     * @param openedTable    Kudu table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    public KuduQuery(RelOptCluster cluster, RelTraitSet traitSet,
                     RelOptTable table, KuduTable openedTable, RelDataType projectRowType) {
        super(cluster, traitSet, table);
        this.openedTable = openedTable;
        this.projectRowType = projectRowType;
        assert getConvention() == KuduRel.CONVENTION;
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
        for (RelOptRule rule : KuduRules.RULES) {
            planner.addRule(rule);
        }
        planner.addRule(KuduToEnumerableConverter.INSTANCE);
    }

    @Override
    public void implement(Implementor impl) {
        // Doesn't call visit child as it is the leaf.
        impl.openedTable = this.openedTable;
        impl.table = this.table;
    }
}
