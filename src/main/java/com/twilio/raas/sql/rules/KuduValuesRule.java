package com.twilio.raas.sql.rules;

import com.twilio.raas.sql.KuduRelNode;
import com.twilio.raas.sql.rel.KuduValuesRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;

public class KuduValuesRule extends ConverterRule {
  public static final KuduValuesRule INSTANCE = new KuduValuesRule();

  private KuduValuesRule() {
    super(LogicalValues.class, Convention.NONE, KuduRelNode.CONVENTION, "KuduValuesRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    Values values = (Values) rel;
    return new KuduValuesRel(
      values.getCluster(),
      values.getRowType(),
      values.getTuples(),
      values.getTraitSet().replace(KuduRelNode.CONVENTION));
  }
}