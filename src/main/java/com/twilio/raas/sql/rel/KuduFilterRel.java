package com.twilio.raas.sql.rel;

import com.twilio.raas.sql.CalciteKuduPredicate;
import com.twilio.raas.sql.KuduRelNode;
import com.twilio.raas.sql.rel.KuduProjectRel.KuduColumnVisitor;

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
import org.apache.kudu.client.KuduPredicate.ComparisonOp;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class KuduFilterRel extends Filter implements KuduRelNode {
    public final List<List<CalciteKuduPredicate>> scanPredicates;
    public final Schema kuduSchema;
    public final boolean useInMemoryFiltering;

    public KuduFilterRel(final RelOptCluster cluster,
                         final RelTraitSet traitSet,
                         final RelNode child,
                         final RexNode condition,
        final List<List<CalciteKuduPredicate>> predicates,
        final Schema kuduSchema,
        boolean useInMemoryFiltering) {
        super(cluster, traitSet, child, condition);
        this.scanPredicates = predicates;
        this.kuduSchema = kuduSchema;
        this.useInMemoryFiltering = useInMemoryFiltering;
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
            this.scanPredicates, kuduSchema, useInMemoryFiltering);
    }

    @Override
    public void implement(final Implementor implementor) {
        implementor.visitChild(0, getInput());
        implementor.predicates.addAll(this.scanPredicates);

        if (useInMemoryFiltering) {
            final KuduColumnVisitor columnExtractor = new KuduColumnVisitor();
            implementor.inMemoryCondition = getCondition();
            implementor.filterProjections = getCondition().accept(columnExtractor);
        }
    }

    @Override
    public RelWriter explainTerms(final RelWriter pw) {
        pw.input("input", getInput());
        int scanCount=1;
        for (final List<CalciteKuduPredicate> scanPredicate : scanPredicates) {
            final StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (final CalciteKuduPredicate predicate : scanPredicate) {
                final String optionalComparator = predicate.operation
                    .map(ComparisonOp::name)
                    .orElse("IN");

                if (first) {
                    first = false;
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
        if (useInMemoryFiltering) {
            pw.item("MemoryFilters", getCondition());
        }
        return pw;
    }
}
