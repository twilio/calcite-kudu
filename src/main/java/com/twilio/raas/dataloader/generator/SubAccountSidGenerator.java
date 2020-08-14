package com.twilio.raas.dataloader.generator;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Random;

public class SubAccountSidGenerator extends SingleColumnValueGenerator<String> {

  public String variableName;
  public String defaultValue;
  public List<String> values;
  private final Random rand = new Random();

  private SubAccountSidGenerator() {
  }

  public SubAccountSidGenerator(final String variableName) {
    this.variableName = variableName;
  }

  @Override
  public synchronized String getColumnValue() {
    if (values == null) {
      String envVariable = System.getenv().get(variableName);
      String value  = envVariable!=null ? envVariable : defaultValue;
      values = Lists.newArrayList("IS_NULL", value);
    }
    return values.get(rand.nextInt(values.size()));
  }

}
