/* Copyright 2020 Twilio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twilio.kudu.dataloader.generator;

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
    if (minValue == 0 && maxValue == 0) {
      minValue = System.currentTimeMillis() - numDaysBefore * DateTimeUtils.MILLIS_PER_DAY;
      maxValue = System.currentTimeMillis();
    }
    return super.getColumnValue();
  }

}
