package com.twilio.raas.dataloader.generator;

public class RandomSidGenerator extends SingleColumnValueGenerator<String> {

  public String sidPrefix;

  private RandomSidGenerator() {
  }

  public RandomSidGenerator(final String sidPrefix) {
    this.sidPrefix = sidPrefix;
  }

  @Override
  public synchronized String getColumnValue() {
    return com.twilio.sids.SidUtil.generateGUID(sidPrefix);
  }

}
