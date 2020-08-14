package com.twilio.raas.dataloader.generator;


import java.util.Random;

public class UniformLongValueGenerator extends SingleColumnValueGenerator<Long> {

  private final Random rand = new Random();
  public long minValue;
  public long maxValue;

  protected UniformLongValueGenerator() {
  }

  public UniformLongValueGenerator(final long minVal, final long maxVal) {
    this.minValue = minVal;
    this.maxValue = maxVal;
  }

  /**
   * Generates a long value between [minValue, maxValue)
   */
  @Override
  public synchronized Long getColumnValue() {
    return minValue + (long) (rand.nextDouble() * ((maxValue-1) - minValue));
  }
}
