package com.twilio.raas.sql.rel.metadata;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;

public class KuduRelMdSelectivity extends RelMdSelectivity {

    private static final KuduRelMdSelectivity INSTANCE = new KuduRelMdSelectivity();
    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.SELECTIVITY.method, INSTANCE);

    public Double getSelectivity(RelNode rel, RelMetadataQuery mq,
                                 RexNode predicate) {
        return guessSelectivity(predicate);
    }

    /**
     * Based on {@link org.apache.calcite.rel.metadata.RelMdUtil}.guessSelectivity. We don't want
     * the filters to be too selective or else a limit won't get pushed down into kudu
     */
    public static double guessSelectivity(
            RexNode predicate) {
        double sel = 1.0;
        if ((predicate == null) || predicate.isAlwaysTrue()) {
            return sel;
        }

        for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
            if (pred.getKind() == SqlKind.IS_NOT_NULL) {
                sel *= .95;
            } else if (pred.isA(SqlKind.EQUALS)) {
                sel *= .50;
            } else if (pred.isA(SqlKind.COMPARISON)) {
                sel *= .75;
            } else {
                sel *= .99;
            }
        }

        return sel;
    }

}
