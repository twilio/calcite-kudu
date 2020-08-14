package com.twilio.raas.dataloader.generator;

import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.util.List;

/**
 * Used to generate values for multiple columns of a row
 */
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type")
public interface MultipleColumnValueGenerator extends ColumnValueGenerator {

  List<String> getColumnNames();

  /**
   * Reset the state so that column values for a new row can be generated
   */
  void reset();

}
