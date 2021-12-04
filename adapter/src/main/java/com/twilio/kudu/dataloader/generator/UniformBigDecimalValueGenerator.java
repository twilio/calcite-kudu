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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Random;

public class UniformBigDecimalValueGenerator extends SingleColumnValueGenerator<BigDecimal> {

  public int precision = 22;
  public int scale = 6;
  public double maxValue;
  private final Random random = new Random();

  private UniformBigDecimalValueGenerator() {
  }

  public UniformBigDecimalValueGenerator(final double maxValue) {
    this.maxValue = maxValue;
  }

  /**
   * Generates a double value between [0, maxValue)
   */
  @Override
  public synchronized BigDecimal getColumnValue() {
    BigDecimal d = new BigDecimal(random.nextDouble() * maxValue, new MathContext(precision));
    return d.setScale(scale, RoundingMode.HALF_UP);
  }

  @Override
  public void initialize() {
  }
}
