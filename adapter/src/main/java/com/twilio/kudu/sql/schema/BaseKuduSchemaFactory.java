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

import com.twilio.kudu.sql.CalciteKuduTable;
import com.twilio.kudu.sql.metadata.KuduTableMetadata;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class BaseKuduSchemaFactory implements SchemaFactory {
  private final Map<String, KuduTableMetadata> kuduTableConfigMap;

  protected Map<String, KuduSchema> schemaCache = new HashMap<>();

  public BaseKuduSchemaFactory(final Map<String, KuduTableMetadata> kuduTableConfigMap) {
    this.kuduTableConfigMap = kuduTableConfigMap;
  }

  @Override
  public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
    final String connectString = (String) operand.get("connect");
    final String enableInserts = (String) operand.get("enableInserts");
    schemaCache.computeIfAbsent(connectString,
        (masterAddresses) -> new KuduSchema(masterAddresses, kuduTableConfigMap, enableInserts));
    return schemaCache.get(connectString);
  }

  public List<CalciteKuduTable> getTables() {
    List<CalciteKuduTable> tableList = new ArrayList<>();
    for (KuduSchema kuduSchema : schemaCache.values()) {
      tableList.addAll(
          kuduSchema.getTableMap().values().stream().map(CalciteKuduTable.class::cast).collect(Collectors.toList()));
    }
    return tableList;
  }

  public Optional<CalciteKuduTable> getTable(String tableName) {
    for (KuduSchema kuduSchema : schemaCache.values()) {
      Optional<CalciteKuduTable> calciteKuduTableOptional = kuduSchema.getTableMap().values().stream()
          .map(CalciteKuduTable.class::cast).filter(t -> t.getKuduTable().getName().equals(tableName)).findFirst();
      if (calciteKuduTableOptional.isPresent())
        return calciteKuduTableOptional;
    }
    return Optional.empty();
  }

}
