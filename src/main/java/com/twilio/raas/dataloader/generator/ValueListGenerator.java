package com.twilio.raas.dataloader.generator;

import java.util.List;
import java.util.Random;

public class ValueListGenerator extends SingleColumnValueGenerator<String> {

  private final Random rand = new Random();
  public List<String> values;

  private ValueListGenerator(){
  }

  public ValueListGenerator(final List<String> values) {
    this.values = values;
  }

  @Override
  public synchronized String getColumnValue() {
    return values.get(rand.nextInt(values.size()));
  }

}
