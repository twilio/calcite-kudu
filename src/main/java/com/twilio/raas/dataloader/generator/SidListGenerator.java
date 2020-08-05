package com.twilio.raas.dataloader.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public class SidListGenerator extends ColumnValueGenerator<String> {

  private final Random rand = new Random();
  private final List<String> values = new ArrayList<>();

  public String sidPrefix;
  public int numValues;

  private SidListGenerator() {
  }

  public SidListGenerator(final String sidPrefix, final int numValues) {
    this.sidPrefix = sidPrefix;
    this.numValues = numValues;
  }

  @Override
  public synchronized String getColumnValue() {
    if (values.isEmpty()) {
      IntStream.range(0, numValues)
        .forEach( index -> values.add(com.twilio.sids.SidUtil.generateGUID(sidPrefix)));
    }
    return values.get(rand.nextInt(values.size()));
  }
}
