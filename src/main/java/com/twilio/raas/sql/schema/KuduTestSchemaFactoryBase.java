package com.twilio.raas.sql.schema;

import com.twilio.kudu.metadata.KuduTableMetadata;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

public abstract class KuduTestSchemaFactoryBase implements SchemaFactory {
    private final Map<String, KuduTableMetadata> kuduTableConfigMap;

    private Map<String, KuduSchema> schemaCache = new HashMap<>();

    public KuduTestSchemaFactoryBase(final Map<String, KuduTableMetadata> kuduTableConfigMap) {
      this.kuduTableConfigMap = kuduTableConfigMap;
    }

    public Schema create(SchemaPlus parentSchema, String name,
                         Map<String, Object> operand) {
        final String connectString = (String) operand.get("connect");
        final String enableInserts = (String) operand.get("enableInserts");
        schemaCache.computeIfAbsent(connectString,
          (masterAddresses) -> new KuduSchema(masterAddresses, kuduTableConfigMap, enableInserts));
        return schemaCache.get(connectString);
    }
}
