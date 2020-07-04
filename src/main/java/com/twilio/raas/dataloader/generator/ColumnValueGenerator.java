package com.twilio.raas.dataloader.generator;

import org.codehaus.jackson.annotate.JsonTypeInfo;

import static org.codehaus.jackson.annotate.JsonTypeInfo.As;
import static org.codehaus.jackson.annotate.JsonTypeInfo.Id;

@JsonTypeInfo(
  use = Id.NAME,
  include = As.PROPERTY,
  property = "type")
public abstract class ColumnValueGenerator<T> {

  public abstract T getColumnValue();

}