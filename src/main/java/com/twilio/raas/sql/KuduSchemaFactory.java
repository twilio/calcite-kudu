package com.twilio.raas.sql;

import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
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
        final Map<String, String> descendingSortedTables = (Map<String, String>) operand.get("descendingSortedTables");
        schemaCache.computeIfAbsent(connectString, (masterAddresses) -> new KuduSchema(masterAddresses, Optional.of(descendingSortedTables)));
        return schemaCache.get(connectString);
    }
}
