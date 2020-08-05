package com.twilio.raas.dataloader.generator;

import java.math.BigDecimal;
import java.util.Random;

public class UniformBigDecimalValueGenerator extends ColumnValueGenerator<BigDecimal> {

  public double maxValue;
  private final Random random = new Random();

  private UniformBigDecimalValueGenerator() {
  }

  public UniformBigDecimalValueGenerator(final double maxValue) {
    this.maxValue = maxValue;
  }

  @Override
  public synchronized BigDecimal getColumnValue() {
    return new BigDecimal(String.valueOf(Math.round(random.nextDouble() * maxValue) / maxValue));
  }

}
