package com.twilio.raas.dataloader.generator;

public class EnvironmentVariableGenerator<T> extends SingleColumnValueGenerator<T> {

  public String variableName;
  public String valueType;
  public T defaultValue;
  private T value;

  private EnvironmentVariableGenerator() {
  }

  public EnvironmentVariableGenerator(final String variableName) {
    this.variableName = variableName;
  }

  @Override
  public synchronized T getColumnValue() {
    if (value == null) {
      String environmentValue = System.getenv().get(variableName);
      try {
        if (environmentValue != null) {
          Class<?> cls = Class.forName(valueType);
          if (String.class.isAssignableFrom(cls)) {
            value = (T) environmentValue;
          } else if (Integer.class.isAssignableFrom(cls)) {
            value = (T) Integer.valueOf(environmentValue);
          } else {
            throw new RuntimeException("EnvironmentVariableGenerator does not support type " + cls);
          }
        } else {
          value = defaultValue;
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Could not find type " + valueType, e);
      }
    }
    return value;
  }

}
