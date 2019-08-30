package com.twilio.raas.sql.rel;

import com.twilio.raas.sql.CalciteKuduPredicate;
import com.twilio.raas.sql.KuduRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class KuduFilterRel extends Filter implements KuduRel {
    public final List<List<CalciteKuduPredicate>> scanPredicates;

    public KuduFilterRel(RelOptCluster cluster,
                         RelTraitSet traitSet,
                         RelNode child,
                         RexNode condition,
                         List<List<CalciteKuduPredicate>> predicates) {
        super(cluster, traitSet, child, condition);
        this.scanPredicates = predicates;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        // Really lower the cost.
        // @TODO: consider if the predicates include primary keys
        // and adjust the cost accordingly.
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }

    public KuduFilterRel copy(RelTraitSet traitSet, RelNode input,
                              RexNode condition) {
        return new KuduFilterRel(getCluster(), traitSet, input, condition,
                                 this.scanPredicates);
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        implementor.predicates.addAll(this.scanPredicates);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw.input("input", getInput());
        int scanCount=1;
        for (List<CalciteKuduPredicate> scanPredicate : scanPredicates) {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (CalciteKuduPredicate predicate : scanPredicate) {
                String optionalComparator = (predicate.operation.isPresent() ?
                        predicate.operation.get().name() : "") ;
                if (first) {
                    first =false;
                }
                else {
                    sb.append(" , ");
                }
                sb.append(predicate.columnName + " " + optionalComparator + " " + predicate.rightHandValue);
            }
            pw.item("Scan " + scanCount++, sb.toString());
        }
        return pw;
    }
}
