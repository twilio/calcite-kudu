package com.twilio.raas.dataloader.generator;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class StatusErrorCodeGenerator implements MultipleColumnValueGenerator {

  private final List<String> columnNames = Arrays.asList("status", "error_code");
  private final List<String> statusList = Arrays.asList("FAILED", "UNDELIVERED", "DELIVERED");
  private final List<Integer> errorCodeList = Arrays.asList(30005, 30008, 30007, 30006, 30010,
    30011);

  private int index;
  private final Random rand = new Random();

  @Override
  public List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  public void reset() {
    index = rand.nextInt(statusList.size());
  }

  @Override
  public Object getColumnValue(String columnName) {
    if (columnName.equals("status")) {
      return statusList.get(index);
    }
    else if (columnName.equals("error_code")) {
      final int errorCodeIndex = rand.nextInt(errorCodeList.size());
      return statusList.get(index).equals("DELIVERED") ? 0 : errorCodeList.get(errorCodeIndex);
    }
    return null;
  }
}
