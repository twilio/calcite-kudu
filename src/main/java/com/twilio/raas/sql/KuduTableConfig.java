package com.twilio.raas.sql;

import java.util.ArrayList;
import java.util.List;

public class KuduTableConfig {
  private final String tableName;
  private List<String> descendingSortedFields;

  public KuduTableConfig(final String tableName) {
    this(tableName, new ArrayList<>());
  }

  public KuduTableConfig(final String tableName, final List<String> descendingSortedFields) {
    this.tableName = tableName;
    this.descendingSortedFields = descendingSortedFields;
  }

  public String getTableName() {
    return tableName;
  }

  public List<String> getDescendingSortedFields() {
    return descendingSortedFields;
  }

  public void setDescendingSortedFields(List<String> descendingSortedFields) {
    this.descendingSortedFields = descendingSortedFields;
  }
}
