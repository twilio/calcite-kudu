package com.twilio.raas.dataloader.generator;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BIPhoneNumberGenerator implements MultipleColumnValueGenerator {

  private final List<String> columnNames =
    Arrays.asList("billable_item", "phonenumber", "to", "from", "calculated_sid");

  final static String SMS_BI = "BIff66a74865ec468592556e5b2cfcff74";
  final static String WIRELESS_BI = "BI0571431529ae8563304b9808b3d26332";
  final static String CALLS_BI = "BIea98c83dcf0a562aabfe1e073b9df8f6";
  final static String PHONENUMBER_BI = "BIf4811c26b8928ca0e66cf318618bd403";

  private final Random rand = new Random();

  private String chosenBi;
  private String to;
  private String from;

  final Map<String, Boolean> bisToPhoneNumbers=  Collections.unmodifiableMap(
    Stream.of(
      new AbstractMap.SimpleEntry<>(WIRELESS_BI, Boolean.FALSE),
      new AbstractMap.SimpleEntry<>(SMS_BI, Boolean.TRUE),
      new AbstractMap.SimpleEntry<>(CALLS_BI, Boolean.TRUE),
      new AbstractMap.SimpleEntry<>(PHONENUMBER_BI, Boolean.FALSE))
      .collect(Collectors.toMap((e) -> e.getKey(), (e) -> e.getValue())));

  final List<String> bis = new ArrayList<>(bisToPhoneNumbers.keySet());
  final List<String> toPhoneNumbers = Arrays.asList("+16302542474", "+18005782340");
  final List<String> fromPhoneNumbers = Arrays.asList("+16302542475", "+18005782341");

  @Override
  public List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  public void reset() {
    chosenBi = bis.get(rand.nextInt(bis.size()));
    if (bisToPhoneNumbers.get(chosenBi).equals(Boolean.TRUE)) {
      to = toPhoneNumbers.get(rand.nextInt(toPhoneNumbers.size()));
      from = fromPhoneNumbers.get(rand.nextInt(fromPhoneNumbers.size()));
    }
  }

  @Override
  public Object getColumnValue(String columnName) {
    if (columnName.equals("billable_item")) {
      return chosenBi;
    }
    else if (bisToPhoneNumbers.get(chosenBi).equals(Boolean.TRUE)) {
      if (columnName.equals("to")) {
        return to;
      }
      else if (columnName.equals("from")) {
        return from;
      }
      else if (columnName.equals("phonenumber")) {
        return (rand.nextBoolean()) ? to : from;
      }
      else if (columnName.equals("calculated_sid")) {
        return com.twilio.sids.SidUtil.generateGUID("SM");
      }
      return null;
    }
    else if (columnNames.contains(columnName)){
      return "IS_NULL";
    }
    return null;
  }
}
