package com.twilio.raas.dataloader.generator;

public class ConstantValueGenerator<T> extends ColumnValueGenerator {

  private T columnValue;

  private ConstantValueGenerator(){
  }

  public ConstantValueGenerator(final T columnValue) {
    this.columnValue = columnValue;
  }

  @Override
  public T getColumnValue() {
    return columnValue;
  }

}
