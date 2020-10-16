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
package com.twilio.kudu.dataloader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import com.twilio.kudu.dataloader.generator.MultipleColumnValueGenerator;
import com.twilio.kudu.dataloader.generator.SingleColumnValueGenerator;
import com.twilio.kudu.dataloader.generator.ConstantValueGenerator;
import com.twilio.kudu.dataloader.generator.EnvironmentVariableGenerator;
import com.twilio.kudu.dataloader.generator.PhoneNumberListGenerator;
import com.twilio.kudu.dataloader.generator.IdGenerator;
import com.twilio.kudu.dataloader.generator.IdListGenerator;
import com.twilio.kudu.dataloader.generator.TimestampGenerator;
import com.twilio.kudu.dataloader.generator.UniformBigDecimalValueGenerator;
import com.twilio.kudu.dataloader.generator.UniformIntegerValueGenerator;
import com.twilio.kudu.dataloader.generator.UniformLongValueGenerator;
import com.twilio.kudu.dataloader.generator.ValueListGenerator;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Scenario {

  private String tableName;
  // number of rows to be written to the fact table
  private int numRows;
  // map from column name to single column value generator
  private Map<String, SingleColumnValueGenerator> columnNameToValueGenerator;

  // list of multiple column value generator
  private List<MultipleColumnValueGenerator> multipleColumnValueGenerators;

  private static ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.registerSubtypes(new NamedType(IdGenerator.class, "RandomSidGenerator"),
        new NamedType(ConstantValueGenerator.class, "ConstantValueGenerator"),
        new NamedType(UniformLongValueGenerator.class, "UniformLongValueGenerator"),
        new NamedType(UniformIntegerValueGenerator.class, "UniformIntegerValueGenerator"),
        new NamedType(IdListGenerator.class, "SidListGenerator"),
        new NamedType(ValueListGenerator.class, "ValueListGenerator"),
        new NamedType(PhoneNumberListGenerator.class, "PhoneNumberListGenerator"),
        new NamedType(UniformBigDecimalValueGenerator.class, "UniformBigDecimalValueGenerator"),
        new NamedType(EnvironmentVariableGenerator.class, "EnvironmentVariableGenerator"),
        new NamedType(TimestampGenerator.class, "TimestampGenerator"));
  }

  private Scenario() {
  }

  public static void registerGenerator(Class<?> c, String name) {
    mapper.registerSubtypes(new NamedType(c, name));
  }

  private Scenario(final String tableName, final Map<String, SingleColumnValueGenerator> columnNameToValueGenerator,
      final int numRows) {
    this.columnNameToValueGenerator = columnNameToValueGenerator;
    this.numRows = numRows;
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public Map<String, SingleColumnValueGenerator> getColumnNameToValueGenerator() {
    return columnNameToValueGenerator;
  }

  public List<MultipleColumnValueGenerator> getMultipleColumnValueGenerators() {
    if (multipleColumnValueGenerators == null) {
      return Collections.emptyList();
    }
    return multipleColumnValueGenerators;
  }

  public int getNumRows() {
    return numRows;
  }

  public static Scenario loadScenario(final File file) throws IOException {
    Scenario scenario = mapper.reader(Scenario.class).readValue(file);
    return scenario;
  }

  public static Scenario loadScenario(final URL url) throws IOException {
    Scenario scenario = mapper.reader(Scenario.class).readValue(url);
    return scenario;
  }

  public static class ScenarioBuilder {

    private final String tableName;
    private int numRows;
    private ImmutableMap.Builder<String, SingleColumnValueGenerator> builder = new ImmutableMap.Builder<>();

    public ScenarioBuilder(final String tableName, final int numRows) {
      this.tableName = tableName;
      this.numRows = numRows;
    }

    public <T> ScenarioBuilder addColumnValueGenerator(String columnName,
        SingleColumnValueGenerator<T> columnValueGenerator) {
      builder.put(columnName, columnValueGenerator);
      return this;
    }

    public Scenario build() {
      return new Scenario(tableName, builder.build(), numRows);
    }

  }

}
