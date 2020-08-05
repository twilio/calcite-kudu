package com.twilio.raas.sql.schema;

import com.twilio.dataset.DatasetUtil;
import com.twilio.raas.sql.CalciteKuduTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class KuduSchemaFactory implements SchemaFactory {
  // Public singleton, per factory contract.
  public static final KuduSchemaFactory INSTANCE = new KuduSchemaFactory();

  private Map<String, KuduSchema> schemaCache = new HashMap<>();

  private KuduSchemaFactory(){
  }

  public Schema create(SchemaPlus parentSchema, String name,
                       Map<String, Object> operand) {
    final String connectString = (String) operand.get("connect");
    final String enableInsertsString = (String) operand.get("enableInserts");
    schemaCache.computeIfAbsent(connectString,
      (masterAddresses) -> new KuduSchema(masterAddresses,
        DatasetUtil.INSTANCE.getKuduTableMetadataMap(), enableInsertsString));
    return schemaCache.get(connectString);
  }

  public List<CalciteKuduTable> getTables() {
    List<CalciteKuduTable> tableList = new ArrayList<>();
    for (KuduSchema kuduSchema : schemaCache.values()) {
      tableList.addAll(
        kuduSchema.getTableMap().values().stream()
        .map(CalciteKuduTable.class::cast)
        .collect(Collectors.toList())
      );
    }
    return tableList;
  }

  public Optional<CalciteKuduTable> getTable(String tableName) {
    for (KuduSchema kuduSchema : schemaCache.values()) {
      Optional<CalciteKuduTable> calciteKuduTableOptional =
        kuduSchema.getTableMap().values().stream()
          .map(CalciteKuduTable.class::cast)
          .filter( t -> t.getKuduTable().getName().equals(tableName))
          .findFirst();
      if (calciteKuduTableOptional.isPresent())
        return calciteKuduTableOptional;
    }
    return Optional.empty();
  }

}