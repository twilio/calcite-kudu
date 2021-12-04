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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public class SidListGenerator extends RandomSidGenerator {

  private final Random rand = new Random();
  private final List<String> values = new ArrayList<>();

  public int numValues;

  private SidListGenerator() {
    super("XX");
  }

  public SidListGenerator(final String sidPrefix, final int numValues) {
    super(sidPrefix);
    this.numValues = numValues;
    this.sidPrefix = sidPrefix;
  }

  @Override
  public String getColumnValue() {
    return values.get(rand.nextInt(values.size()));
  }

  @Override
  public void initialize() {
    if (values.isEmpty()) {
      IntStream.range(0, numValues).forEach(index -> values.add(randomSid()));
    }
  }
}
