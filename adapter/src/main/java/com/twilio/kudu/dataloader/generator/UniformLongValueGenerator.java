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

import java.util.concurrent.ThreadLocalRandom;

public class UniformLongValueGenerator extends SingleColumnValueGenerator<Long> {

  public long minValue;
  public long maxValue;

  protected UniformLongValueGenerator() {
  }

  @Override
  public void initialize() {
  }

  public UniformLongValueGenerator(final long minVal, final long maxVal) {
    this.minValue = minVal;
    this.maxValue = maxVal;
  }

  /**
   * Generates a long value between [minValue, maxValue)
   */
  @Override
  public Long getColumnValue() {
    return minValue + (long) (ThreadLocalRandom.current().nextDouble() * (maxValue - minValue));
  }

}
