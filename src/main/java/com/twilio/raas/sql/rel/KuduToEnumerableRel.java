package com.twilio.raas.sql.rel;

import com.twilio.raas.sql.CalciteKuduPredicate;
import com.twilio.raas.sql.CalciteKuduTable;
import com.twilio.raas.sql.KuduMethod;
import com.twilio.raas.sql.KuduRel;
import com.twilio.raas.sql.KuduScanStats;
import com.twilio.raas.sql.rules.KuduToEnumerableConverter;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;

import java.util.List;
import java.util.stream.Collectors;

public class KuduToEnumerableRel extends ConverterImpl  implements EnumerableRel {
    public KuduToEnumerableRel(RelOptCluster cluster,
                                  RelTraitSet traits,
                                  RelNode input) {
        super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new KuduToEnumerableRel(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    /**
     * This does the bulk of the work, creating a compile-able query to execute.
     */
    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        // Generates a call to "query" with the appropriate fields and predicates
        final BlockBuilder list = new BlockBuilder();
        final KuduRel.Implementor kuduImplementor = new KuduRel.Implementor();
        // This goes and visits the entire tree, setting up kuduImplementor
        // with predicates, fields and limits.
        kuduImplementor.visitChild(0, getInput());
        final RelDataType rowType = getRowType();
        final PhysType physType =
            PhysTypeImpl.of(
                            implementor.getTypeFactory(), rowType,
                            pref.prefer(JavaRowFormat.ARRAY));

        // Now build the Java code that represents the Physical scan of a
        // Kudu Table.
        final Expression predicates = list.append("predicates",
            implementor.stash(kuduImplementor.predicates
                .stream()
                .map(subscan -> {
                        return subscan
                            .stream()
                            .map(p -> {
                                    return new CalciteKuduPredicate(
                                        p.columnName,
                                        p.operation.get(),
                                        p.rightHandValue);
                                })
                            .collect(Collectors.toList());
                    }).collect(Collectors.toList()), List.class));

        final Expression fields =
            list.append("kuduFields",
                implementor.stash(kuduImplementor.kuduProjectedColumns, List.class));

        final Expression limit =
                list.append("limit",
                        Expressions.constant(kuduImplementor.limit));

        final Expression offset =
                list.append("offset",
                        Expressions.constant(kuduImplementor.offset));

        final Expression sorted =
                list.append("sorted",
                        Expressions.constant(kuduImplementor.sorted));

        final Expression table =
            list.append("table",
                    kuduImplementor.table.getExpression(CalciteKuduTable.KuduQueryable.class));

        final Expression scanStats =
            list.append("scanStats", implementor.stash(new KuduScanStats(), KuduScanStats.class));

        Expression enumerable = list.append("enumerable",
                Expressions.call(table,
                        KuduMethod.KUDU_QUERY_METHOD.method, predicates, fields, limit,
                        offset, sorted,
                        Expressions.constant(kuduImplementor.groupByLimited),
                        scanStats));

        Hook.QUERY_PLAN.run(predicates);
        list.add(
                 Expressions.return_(null, enumerable));

        KuduToEnumerableConverter.logger.debug("Created a KuduQueryable " + list.toBlock());
        return implementor.result(physType, list.toBlock());
    }
}
