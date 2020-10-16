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
package com.twilio.kudu.sql.schema;

import com.twilio.kudu.sql.metadata.KuduTableMetadata;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DefaultKuduSchemaFactory extends BaseKuduSchemaFactory {
  // Public singleton, per factory contract.
  public static final DefaultKuduSchemaFactory INSTANCE = new DefaultKuduSchemaFactory();

  private DefaultKuduSchemaFactory() {
    super(new HashMap<>());
  }

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    final String connectString = (String) operand.get("connect");
    final String enableInsertsString = (String) operand.get("enableInserts");

    schemaCache.computeIfAbsent(connectString,
        (masterAddresses) -> new KuduSchema(masterAddresses, loadMetadata(operand), enableInsertsString));
    return schemaCache.get(connectString);
  }

  private Map<String, KuduTableMetadata> loadMetadata(final Map<String, Object> operand) {
    final List<Map<String, Object>> kuduTableConfigs = Optional
        .ofNullable((List<Map<String, Object>>) operand.get("kuduTableConfigs")).orElse(Collections.emptyList());

    final HashMap<String, KuduTableMetadata> tableMap = new HashMap<>(kuduTableConfigs.size());
    for (Map<String, Object> configMap : kuduTableConfigs) {
      final KuduTableMetadata information = new KuduTableMetadata.KuduTableMetadataBuilder()
          .setDescendingOrderedColumnNames((List<String>) configMap.get("descendingSortedFields"))
          .setTimestampColumnName((String) configMap.get("timestampColumnName")).build();
      final String tableName = (String) configMap.get("tableName");
      tableMap.put(tableName, information);
    }
    return tableMap;
  }
}