package com.twilio.raas.dataloader.generator;

import org.apache.calcite.avatica.util.DateTimeUtils;

/**
 * Generates a timestamp randomly between [numDaysAgo, now())
 */
public class TimestampGenerator extends UniformLongValueGenerator {

  public long numDaysBefore;

  private TimestampGenerator() {
    super();
  }

  public TimestampGenerator(final long numDaysBefore) {
    super(System.currentTimeMillis() - numDaysBefore * DateTimeUtils.MILLIS_PER_DAY, System.currentTimeMillis());
  }

  @Override
  public synchronized Long getColumnValue() {
    if (minValue == 0 && maxValue ==0) {
      minValue = System.currentTimeMillis() - numDaysBefore * DateTimeUtils.MILLIS_PER_DAY;
      maxValue = System.currentTimeMillis();
    }
    return super.getColumnValue();
  }

}
