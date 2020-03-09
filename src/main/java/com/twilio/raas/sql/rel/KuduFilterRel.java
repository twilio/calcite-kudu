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
import org.apache.kudu.Schema;

import java.util.List;

public class KuduFilterRel extends Filter implements KuduRel {
    public final List<List<CalciteKuduPredicate>> scanPredicates;
    public final Schema kuduSchema;

    public KuduFilterRel(final RelOptCluster cluster,
                         final RelTraitSet traitSet,
                         final RelNode child,
                         final RexNode condition,
        final List<List<CalciteKuduPredicate>> predicates,
        final Schema kuduSchema) {
        super(cluster, traitSet, child, condition);
        this.scanPredicates = predicates;
        this.kuduSchema = kuduSchema;
    }

    @Override
    public RelOptCost computeSelfCost(final RelOptPlanner planner,
                                      final RelMetadataQuery mq) {
        // Really lower the cost.
        // @TODO: consider if the predicates include primary keys
        // and adjust the cost accordingly.
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }

    public KuduFilterRel copy(final RelTraitSet traitSet, final RelNode input,
                              final RexNode condition) {
        return new KuduFilterRel(getCluster(), traitSet, input, condition,
            this.scanPredicates, kuduSchema);
    }

    @Override
    public void implement(final Implementor implementor) {
        implementor.visitChild(0, getInput());
        implementor.predicates.addAll(this.scanPredicates);
    }

    @Override
    public RelWriter explainTerms(final RelWriter pw) {
        pw.input("input", getInput());
        int scanCount=1;
        for (final List<CalciteKuduPredicate> scanPredicate : scanPredicates) {
            final StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (final CalciteKuduPredicate predicate : scanPredicate) {
                final String optionalComparator = (predicate.operation.isPresent() ?
                        predicate.operation.get().name() : "") ;
                if (first) {
                    first =false;
                }
                else {
                    sb.append(", ");
                }
                sb.append(String.format("%s %s %s",
                        kuduSchema.getColumnByIndex(predicate.columnIdx).getName(),
                        optionalComparator,
                        predicate.rightHandValue));
            }
            pw.item("ScanToken " + scanCount++, sb.toString());
        }
        return pw;
    }
}
