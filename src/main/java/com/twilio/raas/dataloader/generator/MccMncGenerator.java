package com.twilio.raas.dataloader.generator;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MccMncGenerator implements MultipleColumnValueGenerator {

  private final List<String> columnNames = Arrays.asList("mcc", "mnc");
  private final List<String> mccList = Arrays.asList("310", "310", "262");
  private final List<String> mncList = Arrays.asList("004", "016", "01");

  private int index;
  private final Random rand = new Random();

  @Override
  public List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  public void reset() {
    index = rand.nextInt(mccList.size());
  }

  @Override
  public Object getColumnValue(String columnName) {
    if (columnName.equals("mnc")) {
      return mncList.get(index);
    }
    else if (columnName.equals("mcc")) {
      return mccList.get(index);
    }
    return null;
  }
}
