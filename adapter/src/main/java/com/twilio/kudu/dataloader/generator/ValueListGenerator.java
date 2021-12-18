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

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ValueListGenerator extends SingleColumnValueGenerator<String> {

  public List<String> values;

  private ValueListGenerator() {
  }

  public ValueListGenerator(final List<String> values) {
    this.values = values;
  }

  @Override
  public synchronized String getColumnValue() {
    return values.get(ThreadLocalRandom.current().nextInt(values.size()));
  }

  @Override
  public void initialize() {
  }
}
