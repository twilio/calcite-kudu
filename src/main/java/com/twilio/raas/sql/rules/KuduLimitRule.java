package com.twilio.raas.sql.rules;

import com.twilio.raas.sql.KuduRelNode;
import com.twilio.raas.sql.rel.KuduLimitRel;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

public class KuduLimitRule extends RelOptRule {

    public KuduLimitRule() {
        super(operand(EnumerableLimit.class, any()), "KuduLimitRule");
    }

    public RelNode convert(EnumerableLimit limit) {
        final RelTraitSet traitSet =
                limit.getTraitSet().replace(KuduRelNode.CONVENTION);
        return new KuduLimitRel(limit.getCluster(), traitSet,
                convert(limit.getInput(), KuduRelNode.CONVENTION), limit.offset, limit.fetch);
    }

    public void onMatch(RelOptRuleCall call) {
        final EnumerableLimit limit = call.rel(0);
        final RelNode converted = convert(limit);
        if (converted != null) {
            call.transformTo(converted);
        }
    }

}
