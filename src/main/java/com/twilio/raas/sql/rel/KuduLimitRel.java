package com.twilio.raas.sql.rel;

import com.twilio.raas.sql.KuduRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class KuduLimitRel extends SingleRel implements KuduRel {
    public final RexNode offset;
    public final RexNode fetch;

    public KuduLimitRel(RelOptCluster cluster, RelTraitSet traitSet,
                        RelNode input, RexNode offset, RexNode fetch) {
        super(cluster, traitSet, input);
        this.offset = offset;
        this.fetch = fetch;
        assert getConvention() == input.getConvention();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                                RelMetadataQuery mq) {
        return planner.getCostFactory().makeZeroCost();
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
