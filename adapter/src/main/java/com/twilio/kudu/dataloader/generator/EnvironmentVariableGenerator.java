/* Copyright 2020 Twilio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twilio.kudu.dataloader.generator;

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

  @Override
  public void initialize() {
  }
}
