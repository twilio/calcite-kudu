package com.twilio.raas.dataloader;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.twilio.raas.dataloader.generator.ColumnValueGenerator;
import com.twilio.raas.dataloader.generator.ConstantValueGenerator;
import com.twilio.raas.dataloader.generator.PhoneNumberListGenerator;
import com.twilio.raas.dataloader.generator.RandomSidGenerator;
import com.twilio.raas.dataloader.generator.SidListGenerator;
import com.twilio.raas.dataloader.generator.UniformBigDecimalValueGenerator;
import com.twilio.raas.dataloader.generator.UniformIntegerValueGenerator;
import com.twilio.raas.dataloader.generator.UniformLongValueGenerator;
import com.twilio.raas.dataloader.generator.ValueListGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.jsontype.NamedType;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

public class Scenario {

  private String tableName;
  // number of rows to be written to the fact table
  private int numRows;
  // map from column name to value generator
  private Map<String, ColumnValueGenerator> columnNameToValueGenerator;

  private static ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.registerSubtypes(
      new NamedType(RandomSidGenerator.class, "RandomSidGenerator"),
      new NamedType(ConstantValueGenerator.class, "ConstantValueGenerator"),
      new NamedType(UniformLongValueGenerator.class, "UniformLongValueGenerator"),
      new NamedType(UniformIntegerValueGenerator.class, "UniformIntegerValueGenerator"),
      new NamedType(SidListGenerator.class, "SidListGenerator"),
      new NamedType(ValueListGenerator.class, "ValueListGenerator"),
      new NamedType(PhoneNumberListGenerator.class, "PhoneNumberListGenerator"),
      new NamedType(UniformBigDecimalValueGenerator.class, "UniformBigDecimalValueGenerator")
    );
  }

  private Scenario(){
  }

  private Scenario(final String tableName,
                   final Map<String, ColumnValueGenerator> columnNameToValueGenerator,
                   final int numRows) {
    this.columnNameToValueGenerator = columnNameToValueGenerator;
    this.numRows = numRows;
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }

  public Map<String, ColumnValueGenerator> getColumnNameToValueGenerator() {
    return columnNameToValueGenerator;
  }

  public int getNumRows() {
    return numRows;
  }

  public static Scenario loadScenario(final File file) throws IOException {
    Scenario scenario = mapper
      .reader(Scenario.class)
      .readValue(file);
    return scenario;
  }

  public static Scenario loadScenario(final URL url) throws IOException {
    Scenario scenario = mapper
      .reader(Scenario.class)
      .readValue(url);
    return scenario;
  }

  public static class ScenarioBuilder {

    private final String tableName;
    private int numRows;
    private ImmutableMap.Builder<String, ColumnValueGenerator> builder =
      new ImmutableMap.Builder<>();

    public ScenarioBuilder(final String tableName, final int numRows) {
      this.tableName = tableName;
      this.numRows = numRows;
    }

    public <T> ScenarioBuilder addColumnValueGenerator(String columnName,
                                                       ColumnValueGenerator<T> columnValueGenerator) {
      builder.put(columnName, columnValueGenerator);
      return this;
    }

    public Scenario build() {
      return new Scenario(tableName, builder.build(), numRows);
    }

  }

}
