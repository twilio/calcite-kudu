package com.twilio.raas.sql;

import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

public final class KuduSchemaFactory implements SchemaFactory {
    // Public singleton, per factory contract.
    public static final KuduSchemaFactory INSTANCE = new KuduSchemaFactory();

    private Map<String, KuduSchema> schemaCache = new HashMap<>();

    public Schema create(SchemaPlus parentSchema, String name,
                         Map<String, Object> operand) {
        final String connectString = (String) operand.get("connect");
        final Map<String, KuduTableConfig> kuduTableConfigMap = new HashMap<>();
        Optional.ofNullable((List<Map<String, Object>>)operand.get("kuduTableConfigs"))
                .orElse(Collections.<Map<String, Object>>emptyList())
                .forEach(tableConfig ->
                    kuduTableConfigMap.put((String)tableConfig.get("tableName"),
                                            new KuduTableConfig((String)tableConfig.get("tableName"),
                                                                (List<String>)tableConfig.get("descendingSortedFields"))));
        schemaCache.computeIfAbsent(connectString, (masterAddresses) -> new KuduSchema(masterAddresses, kuduTableConfigMap));
        return schemaCache.get(connectString);
    }
}
