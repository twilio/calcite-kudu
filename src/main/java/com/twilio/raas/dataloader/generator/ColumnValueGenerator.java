package com.twilio.raas.dataloader.generator;

public interface ColumnValueGenerator {

  Object getColumnValue(String columnName);

}
