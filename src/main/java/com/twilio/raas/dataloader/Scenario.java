package com.twilio.raas.dataloader;

import com.google.common.collect.ImmutableMap;
import com.twilio.raas.dataloader.generator.ActorSidGenerator;
import com.twilio.raas.dataloader.generator.AuditEventProductMappingGenerator;
import com.twilio.raas.dataloader.generator.BIPhoneNumberGenerator;
import com.twilio.raas.dataloader.generator.MccMncGenerator;
import com.twilio.raas.dataloader.generator.MultipleColumnValueGenerator;
import com.twilio.raas.dataloader.generator.SingleColumnValueGenerator;
import com.twilio.raas.dataloader.generator.ConstantValueGenerator;
import com.twilio.raas.dataloader.generator.EnvironmentVariableGenerator;
import com.twilio.raas.dataloader.generator.PhoneNumberListGenerator;
import com.twilio.raas.dataloader.generator.RandomSidGenerator;
import com.twilio.raas.dataloader.generator.SidListGenerator;
import com.twilio.raas.dataloader.generator.StatusErrorCodeGenerator;
import com.twilio.raas.dataloader.generator.SubAccountSidGenerator;
import com.twilio.raas.dataloader.generator.TimestampGenerator;
import com.twilio.raas.dataloader.generator.UniformBigDecimalValueGenerator;
import com.twilio.raas.dataloader.generator.UniformIntegerValueGenerator;
import com.twilio.raas.dataloader.generator.UniformLongValueGenerator;
import com.twilio.raas.dataloader.generator.ValueListGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.jsontype.NamedType;

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
    mapper.registerSubtypes(
      new NamedType(RandomSidGenerator.class, "RandomSidGenerator"),
      new NamedType(ConstantValueGenerator.class, "ConstantValueGenerator"),
      new NamedType(UniformLongValueGenerator.class, "UniformLongValueGenerator"),
      new NamedType(UniformIntegerValueGenerator.class, "UniformIntegerValueGenerator"),
      new NamedType(SidListGenerator.class, "SidListGenerator"),
      new NamedType(ValueListGenerator.class, "ValueListGenerator"),
      new NamedType(PhoneNumberListGenerator.class, "PhoneNumberListGenerator"),
      new NamedType(UniformBigDecimalValueGenerator.class, "UniformBigDecimalValueGenerator"),
      new NamedType(EnvironmentVariableGenerator.class, "EnvironmentVariableGenerator"),
      new NamedType(SubAccountSidGenerator.class, "SubAccountSidGenerator"),
      new NamedType(TimestampGenerator.class, "TimestampGenerator"),
      new NamedType(ActorSidGenerator.class, "ActorSidGenerator"),
      new NamedType(AuditEventProductMappingGenerator.class, "AuditEventProductMappingGenerator"),
      new NamedType(BIPhoneNumberGenerator.class, "BIAndPhoneNumberGenerator"),
      new NamedType(StatusErrorCodeGenerator.class, "StatusErrorCodeGenerator"),
      new NamedType(MccMncGenerator.class, "MccMncGenerator")
    );
  }

  private Scenario(){
  }

  private Scenario(final String tableName,
                   final Map<String, SingleColumnValueGenerator> columnNameToValueGenerator,
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
    private ImmutableMap.Builder<String, SingleColumnValueGenerator> builder =
      new ImmutableMap.Builder<>();

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
