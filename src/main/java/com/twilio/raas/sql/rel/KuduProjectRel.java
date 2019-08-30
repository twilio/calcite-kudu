package com.twilio.raas.sql.rel;

import com.twilio.raas.sql.KuduRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.List;
import java.util.stream.Collectors;

public class KuduProjectRel extends Project implements KuduRel {
    public KuduProjectRel(RelOptCluster cluster, RelTraitSet traitSet,
                          RelNode input, List<? extends RexNode> projects,
                          RelDataType rowType) {
        super(cluster, traitSet, input, projects, rowType);
    }
    @Override
    public Project copy(RelTraitSet traitSet, RelNode input,
                        List<RexNode> projects, RelDataType rowType) {
        return new KuduProjectRel(getCluster(), traitSet, input, projects,
                                  rowType);
    }
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        final KuduFieldVisitor visitor = new KuduFieldVisitor();
        implementor.kuduProjectedColumns
            .addAll(getNamedProjects()
                    .stream()
                    .map(calcitePair -> {
                            return calcitePair.left.accept(visitor);
                        })
                    .collect(Collectors.toList()));
    }

    public static class KuduFieldVisitor extends RexVisitorImpl<Integer> {
        public KuduFieldVisitor() {
            super(true);
        }

        @Override
        public Integer visitInputRef(RexInputRef inputRef) {
            // @TODO: this is so lame.
            return inputRef.getIndex();
        }
    }
}
