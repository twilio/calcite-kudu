package com.twilio.raas.sql.rules;

import com.twilio.raas.sql.KuduRelNode;
import com.twilio.raas.sql.KuduWrite;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import java.util.Arrays;

public class KuduWriteRule extends ConverterRule {
  public static final KuduWriteRule INSTANCE = new KuduWriteRule();

  private KuduWriteRule() {
    super(
      KuduWrite.class,
      KuduRelNode.CONVENTION,
      KuduRelNode.CONVENTION,
      "KuduWriteRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    final RelNode convertedInput = convert(rel.getInput(0), rel.getTraitSet());
    return rel.copy(rel.getTraitSet(), Arrays.asList(convertedInput));
  }
}