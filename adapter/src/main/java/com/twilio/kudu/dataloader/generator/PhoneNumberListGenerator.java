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
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

public class PhoneNumberListGenerator extends SingleColumnValueGenerator<String> {

  public int numValues;
  private List<String> values = new ArrayList<>();

  private PhoneNumberListGenerator() {
  }

  public PhoneNumberListGenerator(final int numValues) {
    this.numValues = numValues;
  }

  private String generatePhoneNumber() {
    int leftLimit = 48; // 0
    int rightLimit = 57; // 9
    int targetStringLength = 10;
    String phoneNumber = ThreadLocalRandom.current().ints(leftLimit, rightLimit + 1).limit(targetStringLength)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    return "+1" + phoneNumber;
  }

  @Override
  public String getColumnValue() {
    return values.get(ThreadLocalRandom.current().nextInt(values.size()));
  }

  @Override
  public void initialize() {
    if (values.isEmpty()) {
      IntStream.range(0, numValues).forEach(index -> values.add(generatePhoneNumber()));
    }
  }

}
