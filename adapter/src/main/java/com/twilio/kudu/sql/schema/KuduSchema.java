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

import com.twilio.kudu.sql.metadata.CubeTableInfo;
import com.twilio.kudu.sql.metadata.KuduTableMetadata;
import com.twilio.kudu.sql.CalciteKuduTableBuilder;
import com.twilio.kudu.sql.CalciteModifiableKuduTable;
import com.twilio.kudu.sql.CalciteKuduTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduTable;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public final class KuduSchema extends AbstractSchema {

  private static final Logger logger = LoggerFactory.getLogger(KuduSchema.class);

  private final AsyncKuduClient client;
  private final Map<String, KuduTableMetadata> kuduTableMetadataMap;
  private Optional<Map<String, Table>> cachedTableMap = Optional.empty();

  // properties
  public static String KUDU_CONNECTION_STRING = "connect";
  public static String ENABLE_INSERTS_FLAG = "enableInserts";
  public static String DISABLE_CUBE_AGGREGATIONS = "disableCubeAggregation";
  public static String CREATE_DUMMY_PARTITION_FLAG = "createDummyPartition";

  public final boolean enableInserts;
  public final boolean disableCubeAggregation;
  public final boolean createDummyPartition;

  public KuduSchema(final String connectString, final Map<String, KuduTableMetadata> kuduTableMetadataMap,
      final Map<String, Object> propertyMap) {
    this.client = new AsyncKuduClient.AsyncKuduClientBuilder(connectString).build();
    this.kuduTableMetadataMap = kuduTableMetadataMap;
    // We disable inserts by default as this feature has not been thoroughly tested
    this.enableInserts = Boolean.valueOf((String) propertyMap.getOrDefault(ENABLE_INSERTS_FLAG, "false"));
    // If set to true CubeMutationState does not compute aggregations in order to
    // speed up the
    // DataLoader (useful only for performance testing)
    this.disableCubeAggregation = Boolean
        .valueOf((String) propertyMap.getOrDefault(DISABLE_CUBE_AGGREGATIONS, "false"));
    this.createDummyPartition = Boolean.valueOf((String) propertyMap.getOrDefault(CREATE_DUMMY_PARTITION_FLAG, "true"));
  }

  public void clearCachedTableMap() {
    cachedTableMap = Optional.empty();
  }

  @Override
  protected Map<String, Table> getTableMap() {
    if (cachedTableMap.isPresent()) {
      return cachedTableMap.get();
    }

    HashMap<String, Table> tableMap = new HashMap<>();
    final List<String> tableNames;
    try {
      tableNames = this.client.getTablesList().join().getTablesList();
    } catch (Exception threadInterrupted) {
      return Collections.emptyMap();
    }

    Map<String, List<CubeTableInfo>> factToCubeListMap = new HashMap<>();
    // populate the cubetables which were created using DDL statements.
    for (String tableName : tableNames) {
      // cube table
      String[] tableNameSplit = tableName.split("-");
      if (tableNameSplit.length == 4 && tableName.endsWith("Aggregation")) {
        // Cube table name is of the form TableName-CubeName-Interval-Aggregation
        CubeTableInfo cubeTableInfo = new CubeTableInfo(tableName,
            CubeTableInfo.EventTimeAggregationType.valueOf(tableNameSplit[2].toLowerCase()));
        String factTableName = tableNameSplit[0];
        if (!factToCubeListMap.containsKey(factTableName)) {
          factToCubeListMap.put(factTableName, new ArrayList<>());
        }
        factToCubeListMap.get(factTableName).add(cubeTableInfo);
        logger.info("Added cubetable info to factToCubeListMap " + "Cubetablename: " + cubeTableInfo.tableName +
                "EventAggregationType: " + cubeTableInfo.eventTimeAggregationType
        +"FactTableName: " + factTableName);

        logger.error("Added cubetable info to factToCubeListMap " + "Cubetablename: " + cubeTableInfo.tableName +
                "EventAggregationType: " + cubeTableInfo.eventTimeAggregationType
                +"FactTableName: " + factTableName);

        System.out.println("Added cubetable info to factToCubeListMap " + "Cubetablename: " + cubeTableInfo.tableName +
                "EventAggregationType: " + cubeTableInfo.eventTimeAggregationType
                +"FactTableName: " + factTableName);
      }
    }

    // populate kudutableMetadatMap for fact tables that were created using DDL
    // statements.
    for (String tableName : tableNames) {
      List<String> descendingOrderedColumns = new ArrayList<>();
      String timeStampColumnName = "";
      if (!tableName.endsWith("Aggregation")) {
        try {
          KuduTable kuduTable = this.client.openTable(tableName).join();
          for (ColumnSchema columnSchema : kuduTable.getSchema().getColumns()) {
            logger.info("populating kudutableMetadatMap for fact tables created with DDLS:  " + columnSchema.getName());
            logger.error("populating kudutableMetadatMap for fact tables created with DDLS:  " + columnSchema.getName());
            String comment = columnSchema.getComment();
            JSONObject jsonObject = getJsonObject(comment);
            if (!comment.isEmpty() && jsonObject != null) {
              if (jsonObject.has("isTimeStampColumn") && jsonObject.getBoolean("isTimeStampColumn")) {
                timeStampColumnName = columnSchema.getName();
              }

              if (jsonObject.has("isDescendingSortOrder") && jsonObject.getBoolean("isDescendingSortOrder")) {
                descendingOrderedColumns.add(columnSchema.getName());
              }
            }
          }
        } catch (Exception e) {
          logger.error("Unable to open table " + tableName, e);
        }

        if (!timeStampColumnName.isEmpty()) {
          if (factToCubeListMap.get(tableName) != null) {
            this.kuduTableMetadataMap.put(tableName,
                new KuduTableMetadata.KuduTableMetadataBuilder().setTimestampColumnName(timeStampColumnName)
                    .setCubeTableInfoList(factToCubeListMap.get(tableName))
                    .setDescendingOrderedColumnNames(descendingOrderedColumns).build());
          } else {
            this.kuduTableMetadataMap.put(tableName,
                new KuduTableMetadata.KuduTableMetadataBuilder().setTimestampColumnName(timeStampColumnName)
                    .setDescendingOrderedColumnNames(descendingOrderedColumns).build());
          }
        }
      }
    }

    // create CalciteKuduTables
    for (Map.Entry<String, KuduTableMetadata> entry : kuduTableMetadataMap.entrySet()) {
      KuduTableMetadata kuduTableMetadata = entry.getValue();
      final List<String> descendingOrderedColumnNames = kuduTableMetadata.getDescendingOrderedColumnNames();

      // create any cube tables first
      List<CalciteKuduTable> cubeTableList = new ArrayList<>(5);
      for (CubeTableInfo cubeTableInfo : kuduTableMetadata.getCubeTableInfo()) {
        Optional<KuduTable> cubeTableOptional = openKuduTable(cubeTableInfo.tableName);
        cubeTableOptional.ifPresent(kuduTable -> {
          final CalciteKuduTableBuilder builder = new CalciteKuduTableBuilder(kuduTable, client)
              .setEnableInserts(enableInserts).setDisableCubeAggregation(disableCubeAggregation)
              .setTableType(com.twilio.kudu.sql.TableType.CUBE)
              .setEventTimeAggregationType(cubeTableInfo.eventTimeAggregationType);
          setDescendingFieldIndices(builder, descendingOrderedColumnNames, kuduTable);
          setTimestampColumnIndex(builder, kuduTableMetadata.getTimestampColumnName(), kuduTable);
          CalciteKuduTable calciteKuduTable = builder.build();
          tableMap.put(cubeTableInfo.tableName, calciteKuduTable);
          cubeTableList.add(calciteKuduTable);
        });
      }

      // create the fact table
      String factTableName = entry.getKey();
      Optional<KuduTable> factTableOptional = openKuduTable(factTableName);
      factTableOptional.ifPresent(kuduTable -> {
        final CalciteKuduTableBuilder builder = new CalciteKuduTableBuilder(kuduTable, client)
            .setEnableInserts(enableInserts).setDisableCubeAggregation(disableCubeAggregation)
            .setTableType(com.twilio.kudu.sql.TableType.FACT).setCubeTables(cubeTableList);
        setDescendingFieldIndices(builder, descendingOrderedColumnNames, kuduTable);
        setTimestampColumnIndex(builder, kuduTableMetadata.getTimestampColumnName(), kuduTable);
        CalciteKuduTable factTable = builder.build();
        tableMap.put(factTableName, factTable);

        // create cube maintainer for each cube table
        if (enableInserts) {
          for (CalciteKuduTable cubeTable : cubeTableList) {
            ((CalciteModifiableKuduTable) cubeTable).createCubeMaintainer(factTable);
          }
        }
      });
    }

    // load remaining tables (dimension tables and system tables)
    for (String tableName : tableNames) {
      if (!tableMap.containsKey(tableName)) {
        if (tableName.startsWith("System")) {
          Optional<KuduTable> kuduTableOptional = openKuduTable(tableName);
          kuduTableOptional.ifPresent(kuduTable -> {
            createCalciteTable(tableMap, kuduTable, com.twilio.kudu.sql.TableType.SYSTEM);
          });
        } else {
          Optional<KuduTable> kuduTableOptional = openKuduTable(tableName);
          kuduTableOptional.ifPresent(kuduTable -> {
            createCalciteTable(tableMap, kuduTable, com.twilio.kudu.sql.TableType.DIMENSION);
          });
        }
      }
    }

    if (!tableMap.isEmpty()) {
      cachedTableMap = Optional.of(tableMap);
    }
    return tableMap;
  }

  private void createCalciteTable(HashMap<String, Table> tableMap, KuduTable kuduTable,
      com.twilio.kudu.sql.TableType tableType) {
    final CalciteKuduTableBuilder builder = new CalciteKuduTableBuilder(kuduTable, client)
        .setEnableInserts(enableInserts).setTableType(tableType);
    CalciteKuduTable calciteKuduTable = builder.build();
    tableMap.put(kuduTable.getName(), calciteKuduTable);
  }

  private Optional<KuduTable> openKuduTable(String tableName) {
    try {
      return Optional.of(client.openTable(tableName).join());
    } catch (Exception e) {
      logger.trace("Unable to open table " + tableName, e);
      return Optional.empty();
    }
  }

  private void setDescendingFieldIndices(CalciteKuduTableBuilder builder, List<String> descendingOrderedColumnNames,
      KuduTable kuduTable) {
    final List<Integer> descendingOrderedColumnIndices = descendingOrderedColumnNames.stream()
        .map(name -> kuduTable.getSchema().getColumnIndex(name)).collect(Collectors.toList());
    builder.setDescendingOrderedFieldIndices(descendingOrderedColumnIndices);
  }

  private void setTimestampColumnIndex(CalciteKuduTableBuilder builder, String timestampColumnName,
      KuduTable kuduTable) {
    if (timestampColumnName != null) {
      builder.setTimestampColumnIndex(kuduTable.getSchema().getColumnIndex(timestampColumnName));
    }
  }

  public AsyncKuduClient getClient() {
    return client;
  }

  private JSONObject getJsonObject(String comment) {
    try {
      return new JSONObject(comment);
    } catch (JSONException ex) {
      return null;
    }
  }

}
