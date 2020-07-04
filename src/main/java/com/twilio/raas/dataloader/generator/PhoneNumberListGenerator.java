package com.twilio.raas.dataloader.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

public class PhoneNumberListGenerator extends ColumnValueGenerator<String> {

  private final Random random = new Random();

  public int numValues;
  private List<String> values = new ArrayList<>();

  private String generatePhoneNumber() {
    int leftLimit = 48; // 0
    int rightLimit = 57; // 9
    int targetStringLength = 10;
    String phoneNumber = random.ints(leftLimit, rightLimit + 1)
      .limit(targetStringLength)
      .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
      .toString();
    return "+1" + phoneNumber;
  }

  private PhoneNumberListGenerator(){
  }

  public PhoneNumberListGenerator(final int numValues) {
    this.numValues = numValues;
  }

  @Override
  public String getColumnValue() {
    if (values.isEmpty()) {
      IntStream.range(0, numValues)
        .forEach( index -> values.add(generatePhoneNumber()));
    }
    return values.get(random.nextInt(values.size()));
  }
}
