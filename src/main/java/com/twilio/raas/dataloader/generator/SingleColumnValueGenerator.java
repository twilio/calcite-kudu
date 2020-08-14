package com.twilio.raas.dataloader.generator;

import org.codehaus.jackson.annotate.JsonTypeInfo;

import static org.codehaus.jackson.annotate.JsonTypeInfo.As;
import static org.codehaus.jackson.annotate.JsonTypeInfo.Id;

/**
 * Used to generate a value for a one column of a row
 */
@JsonTypeInfo(
  use = Id.NAME,
  include = As.PROPERTY,
  property = "type")
public abstract class SingleColumnValueGenerator<T> implements ColumnValueGenerator {

  public abstract T getColumnValue();

  @Override
  public Object getColumnValue(String columnName) {
    return getColumnValue();
  }

}