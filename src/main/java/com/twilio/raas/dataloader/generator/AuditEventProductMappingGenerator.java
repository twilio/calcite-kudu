package com.twilio.raas.dataloader.generator;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class AuditEventProductMappingGenerator implements MultipleColumnValueGenerator {

  private final Random rand = new Random();
  private final JsonFactory factory = new JsonFactory();
  private final ObjectMapper mapper = new ObjectMapper(factory);

  private final List<String> columnNames = Arrays.asList("resource_type", "event_type");
  private final List<JsonNode> auditEventProductMappings = new ArrayList<>();
  private int index;

  @Override
  public List<String> getColumnNames() {
    return columnNames;
  }

  @Override
  public void reset() {
    index = rand.nextInt(auditEventProductMappings.size());
  }

  public AuditEventProductMappingGenerator() throws IOException {
      final InputStream fileStream = this.getClass().getResourceAsStream("/product_auditEvent.json");
    final Iterator<JsonNode> collectionOfAuditEventProductMappings =
      mapper.readTree(factory.createJsonParser(fileStream)).getElements();
    // add to list for consumption in creation of txns
    collectionOfAuditEventProductMappings.forEachRemaining(auditEventProductMappings::add);
  }

  @Override
  public Object getColumnValue(String columnName) {
    if (columnName.equals("resource_type") || columnName.equals("event_type")) {
      final JsonNode randomAEProductMapping = auditEventProductMappings.get(index);
      return randomAEProductMapping.get(columnName).getTextValue();
    }
    return null;
  }

}
