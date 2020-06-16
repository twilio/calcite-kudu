package com.twilio.raas.sql.rel;

import com.twilio.raas.sql.KuduRelNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.stream.Collectors;

public class KuduProjectValuesRel extends Project implements KuduRelNode {

  public KuduProjectValuesRel(RelOptCluster cluster, RelTraitSet traitSet,
                        RelNode input, List<? extends RexNode> projects,
                        RelDataType rowType) {
    super(cluster, traitSet, input, projects, rowType);
  }
  @Override
  public Project copy(RelTraitSet traitSet, RelNode input,
                      List<RexNode> projects, RelDataType rowType) {
    return new KuduProjectValuesRel(getCluster(), traitSet, input, projects, rowType);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.columnIndexes = rowType.getFieldList().stream()
      .map(RelDataTypeField::getIndex)
      .collect(Collectors.toList());
    implementor.numBindExpressions = getProjects().size();
  }

}
