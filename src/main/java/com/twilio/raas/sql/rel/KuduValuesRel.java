package com.twilio.raas.sql.rel;

import com.google.common.collect.ImmutableList;
import com.twilio.raas.sql.KuduRelNode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;

import java.util.stream.Collectors;

public class KuduValuesRel extends Values implements KuduRelNode {

  public KuduValuesRel(
    RelOptCluster cluster,
    RelDataType rowType,
    ImmutableList<ImmutableList<RexLiteral>> tuples,
    RelTraitSet traits) {
    super(cluster, rowType, tuples, traits);
  }

  @Override
  public void implement(Implementor implementor) {
    implementor.columnIndexes = rowType.getFieldList().stream()
      .map(RelDataTypeField::getIndex)
      .collect(Collectors.toList());
    implementor.tuples = tuples;
  }

}
