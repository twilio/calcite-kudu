package com.twilio.raas.dataloader.generator;

import java.util.Random;

public class UniformIntegerValueGenerator extends ColumnValueGenerator<Integer> {

  private final Random random = new Random();
  public int minValue;
  public int maxValue;

  private UniformIntegerValueGenerator(){
  }

  public UniformIntegerValueGenerator(final int minVal, final int maxVal) {
    this.minValue = minVal;
    this.maxValue = maxVal;
  }

  @Override
  public synchronized Integer getColumnValue() {
    return minValue + (int) (random.nextDouble() * (maxValue - minValue));
  }
  
}
