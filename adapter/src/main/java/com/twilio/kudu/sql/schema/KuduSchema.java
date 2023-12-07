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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.MaterializedViewTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduTable;

import com.twilio.kudu.sql.CalciteKuduTable;
import com.twilio.kudu.sql.CalciteKuduTableBuilder;
import com.twilio.kudu.sql.CalciteModifiableKuduTable;
import com.twilio.kudu.sql.KuduQuery;
import com.twilio.kudu.sql.metadata.CubeTableInfo;
import com.twilio.kudu.sql.metadata.KuduTableMetadata;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class KuduSchema extends AbstractSchema {

  private static final Logger logger = LoggerFactory.getLogger(KuduSchema.class);

  private final AsyncKuduClient client;
  private final Map<String, KuduTableMetadata> kuduTableMetadataMap;
  private Optional<Map<String, Table>> cachedTableMap = Optional.empty();
  private Map<String, String> materializedViewSqls = new ConcurrentHashMap<>();
  private final SchemaPlus parentSchema;
  private final String name;
  private final Hook.Closeable addMaterializationsHook;
  private final Hook.Closeable assignHintsHook;

  // properties
  public static String KUDU_CONNECTION_STRING = "connect";
  public static String ENABLE_INSERTS_FLAG = "enableInserts";
  public static String DISABLE_CUBE_AGGREGATIONS = "disableCubeAggregation";
  public static String CREATE_DUMMY_PARTITION_FLAG = "createDummyPartition";
  public static String READ_SNAPSHOT_TIME_DIFFERENCE = "readSnapshotTimeDifference";
  public static String DISABLE_SCHEMA_CACHE = "disableSchemaCache";
  public static String DISABLE_MATERIALIZED_VIEWS = "disableMaterializedViews";

  public final boolean enableInserts;
  public final boolean disableCubeAggregation;
  public final boolean disableMaterializedViews;
  public final boolean createDummyPartition;
  public final long readSnapshotTimeDifference;

  public KuduSchema(final SchemaPlus parentSchema, final String name, final String connectString,
      final Map<String, KuduTableMetadata> kuduTableMetadataMap, final Map<String, Object> propertyMap) {
    this.client = new AsyncKuduClient.AsyncKuduClientBuilder(connectString).build();
    this.kuduTableMetadataMap = new HashMap<>(kuduTableMetadataMap);
    // We disable inserts by default as this feature has not been thoroughly tested
    this.enableInserts = Boolean.valueOf((String) propertyMap.getOrDefault(ENABLE_INSERTS_FLAG, "false"));
    // If set to true CubeMutationState does not compute aggregations in order to
    // speed up the
    // DataLoader (useful only for performance testing)
    this.disableCubeAggregation = Boolean
        .valueOf((String) propertyMap.getOrDefault(DISABLE_CUBE_AGGREGATIONS, "false"));
    this.disableMaterializedViews = Boolean
        .valueOf((String) propertyMap.getOrDefault(DISABLE_MATERIALIZED_VIEWS, "false"));
    this.createDummyPartition = Boolean.valueOf((String) propertyMap.getOrDefault(CREATE_DUMMY_PARTITION_FLAG, "true"));
    this.readSnapshotTimeDifference = Long
        .valueOf((String) propertyMap.getOrDefault(READ_SNAPSHOT_TIME_DIFFERENCE, "0"));
    this.parentSchema = parentSchema;
    this.name = name;
    this.addMaterializationsHook = disableMaterializedViews ? null : prepareAddMaterializationsHook();
    this.assignHintsHook = Hook.SQL2REL_CONVERTER_CONFIG_BUILDER.add(node -> {
      KuduSchema.this.assignHints(((Holder<SqlToRelConverter.Config>) node));
    });
  }

  public void clearCachedTableMap() {
    cachedTableMap = Optional.empty();
  }

  private Pair<String, List<String>> getTableMetadataFromComments(String tableName) {
    String timeStampColumnName = null;
    List<String> descendingOrderedColumns = new ArrayList<>();
    try {
      KuduTable kuduTable = this.client.openTable(tableName).join();
      for (ColumnSchema columnSchema : kuduTable.getSchema().getColumns()) {
        String comment = columnSchema.getComment();
        if (!comment.isEmpty()) {
          JSONObject jsonObject = new JSONObject(comment);
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
    return new Pair<>(timeStampColumnName, descendingOrderedColumns);
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

    // if the KuduSchemaFactory did not populate the kuduTableMetadataMap examine
    // the metadata stored in the column comment field by the DDL statements to
    // generate the kuduTableMetadataMap
    if (kuduTableMetadataMap.isEmpty()) {
      Map<String, List<CubeTableInfo>> factToCubeListMap = new HashMap<>();
      // populate the cube tables which were created using DDL statements.
      for (String tableName : tableNames) {
        // cube table
        String[] tableNameSplit = tableName.split("-");
        // cube table name is of the form TableName-CubeName-Interval-Aggregation
        if (tableNameSplit.length == 4 && tableName.endsWith("Aggregation")) {
          String factTableName = tableNameSplit[0];
          Pair<String, List<String>> metadataFromComments = getTableMetadataFromComments(tableName);
          if (metadataFromComments.left == null && metadataFromComments.right.isEmpty()) {
            // if the view was created before we added support for ORDER BY set the metadata
            // based on the fact table ROW_TIMESTAMP column
            metadataFromComments = getTableMetadataFromComments(factTableName);
          }
          CubeTableInfo cubeTableInfo = new CubeTableInfo(tableName,
              CubeTableInfo.EventTimeAggregationType.valueOf(tableNameSplit[2].toLowerCase()),
              metadataFromComments.right, metadataFromComments.left);
          if (!factToCubeListMap.containsKey(factTableName)) {
            factToCubeListMap.put(factTableName, new ArrayList<>());
          }
          factToCubeListMap.get(factTableName).add(cubeTableInfo);
        }
      }

      for (String tableName : tableNames) {
        if (!tableName.endsWith("Aggregation")) {
          Pair<String, List<String>> metadataFromComments = getTableMetadataFromComments(tableName);
          if (metadataFromComments.left != null) {
            if (factToCubeListMap.get(tableName) != null) {
              this.kuduTableMetadataMap.put(tableName,
                  new KuduTableMetadata.KuduTableMetadataBuilder().setTimestampColumnName(metadataFromComments.left)
                      .setCubeTableInfoList(factToCubeListMap.get(tableName))
                      .setDescendingOrderedColumnNames(metadataFromComments.right).build());
            } else {
              this.kuduTableMetadataMap.put(tableName,
                  new KuduTableMetadata.KuduTableMetadataBuilder().setTimestampColumnName(metadataFromComments.left)
                      .setDescendingOrderedColumnNames(metadataFromComments.right).build());
            }
          }
        }
      }
    }

    // create CalciteKuduTables
    for (Map.Entry<String, KuduTableMetadata> entry : kuduTableMetadataMap.entrySet()) {
      KuduTableMetadata kuduTableMetadata = entry.getValue();
      String factTableName = entry.getKey();
      // create any cube tables first
      List<CalciteKuduTable> cubeTableList = new ArrayList<>(5);
      for (CubeTableInfo cubeTableInfo : kuduTableMetadata.getCubeTableInfo()) {
        Optional<KuduTable> cubeTableOptional = openKuduTable(cubeTableInfo.tableName);
        cubeTableOptional.ifPresent(cubeKuduTable -> {
          final CalciteKuduTableBuilder builder = new CalciteKuduTableBuilder(cubeKuduTable, client)
              .setEnableInserts(enableInserts).setDisableCubeAggregation(disableCubeAggregation)
              .setReadSnapshotTimeDifference(readSnapshotTimeDifference)
              .setTableType(com.twilio.kudu.sql.TableType.CUBE)
              .setEventTimeAggregationType(cubeTableInfo.eventTimeAggregationType);
          setDescendingFieldIndices(builder, cubeTableInfo.descendingOrderedColumnNames, cubeKuduTable);
          setTimestampColumnIndex(builder, cubeTableInfo.timestampColumnName, cubeKuduTable);
          CalciteKuduTable calciteKuduTable = builder.build();
          tableMap.put(cubeTableInfo.tableName, calciteKuduTable);
          cubeTableList.add(calciteKuduTable);

          StringBuilder queryBuilder = new StringBuilder("SELECT ");
          // Add the group by columns to the SELECT
          List<String> groupByCols = new ArrayList<>();
          Schema cubeSchema = cubeKuduTable.getSchema();
          for (int i = 0; i < cubeSchema.getPrimaryKeyColumnCount(); ++i) {
            String colName = cubeSchema.getColumnByIndex(i).getName();
            if (colName.equals(cubeTableInfo.timestampColumnName)) {
              groupByCols
                  .add("FLOOR(\"" + colName.toUpperCase() + "\" TO " + cubeTableInfo.eventTimeAggregationType + ")");
            } else {
              groupByCols.add("\"" + colName.toUpperCase() + "\"");
            }
          }
          queryBuilder.append(Util.toString(groupByCols, "", ", ", ""));
          queryBuilder.append(" , ");

          // Add the aggregate expressions columns to the SELECT
          List<String> aggExprs = new ArrayList<>();
          for (int i = cubeSchema.getPrimaryKeyColumnCount(); i < cubeSchema.getColumnCount(); ++i) {
            String columnName = cubeSchema.getColumnByIndex(i).getName();
            String aggregateFunction = columnName.substring(0, columnName.indexOf("_"));
            String colName = columnName.substring(columnName.indexOf("_") + 1);
            if (aggregateFunction.equalsIgnoreCase("COUNT")) {
              aggExprs.add("COUNT(*)");
            } else {
              aggExprs.add(aggregateFunction + "(\"" + colName.toUpperCase() + "\")");
            }
          }
          queryBuilder.append(Util.toString(aggExprs, "", ", ", ""));

          queryBuilder.append(" FROM \"").append(factTableName).append("\"");

          // Add the group by expressions to the query
          queryBuilder.append(" GROUP BY ");
          List<String> groupByExprs = new ArrayList<>();
          for (int i = 0; i < cubeSchema.getPrimaryKeyColumnCount(); ++i) {
            String colName = cubeSchema.getColumnByIndex(i).getName();
            if (colName.equals(cubeTableInfo.timestampColumnName)) {
              groupByExprs
                  .add("FLOOR(\"" + colName.toUpperCase() + "\" TO " + cubeTableInfo.eventTimeAggregationType + ")");
            } else {
              groupByExprs.add("\"" + colName.toUpperCase() + "\"");
            }
          }
          queryBuilder.append(Util.toString(groupByExprs, "", ", ", ""));

          // TODO figure out why adding the ORDER BY leads to the view not being chosen
          // Add the order by expressions to the query
//          queryBuilder.append(" ORDER BY ");
//          List<String> orderByExprs = new ArrayList<>();
//          for (int i = 0; i < cubeSchema.getPrimaryKeyColumnCount(); ++i) {
//            String colName = cubeSchema.getColumnByIndex(i).getName();
//            String orderBy;
//            if (colName.equals(cubeTableInfo.timestampColumnName)) {
//              orderBy = "FLOOR(\"" + colName.toUpperCase() + "\" TO " + cubeTableInfo.eventTimeAggregationType + ")";
//            } else {
//              orderBy = "\"" + colName.toUpperCase() + "\"";
//            }
//            if
//            (cubeTableInfo.descendingOrderedColumnNames.contains(colName)) {
//              orderBy = orderBy + " DESC";
//            }
//            orderByExprs.add(orderBy);
//          }
//          queryBuilder.append(Util.toString(orderByExprs, "", ", ", ""));

          // Parse and unparse the view query to get properly quoted field names
          String query = queryBuilder.toString();
          SqlParser.Config parserConfig = SqlParser.config().withUnquotedCasing(Casing.UNCHANGED);

          SqlNode parsedQuery;
          try {
            parsedQuery = SqlParser.create(query, parserConfig).parseQuery();
          } catch (Exception e) {
            logger.error("Could not parse query {} for Kudu cube {}", query, cubeTableInfo.tableName);
            throw new RuntimeException(e);
          }

          final StringBuilder buf = new StringBuilder(query.length());
          final SqlWriterConfig config = SqlPrettyWriter.config().withAlwaysUseParentheses(true);
          final SqlWriter writer = new SqlPrettyWriter(config, buf);
          parsedQuery.unparse(writer, 0, 0);
          query = buf.toString();

          materializedViewSqls.put(cubeTableInfo.tableName, query);
        });
      }

      // create the fact table
      Optional<KuduTable> factTableOptional = openKuduTable(factTableName);
      factTableOptional.ifPresent(kuduTable -> {
        final CalciteKuduTableBuilder builder = new CalciteKuduTableBuilder(kuduTable, client)
            .setEnableInserts(enableInserts).setDisableCubeAggregation(disableCubeAggregation)
            .setReadSnapshotTimeDifference(readSnapshotTimeDifference).setTableType(com.twilio.kudu.sql.TableType.FACT)
            .setCubeTables(cubeTableList);
        setDescendingFieldIndices(builder, kuduTableMetadata.getDescendingOrderedColumnNames(), kuduTable);
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
        .setEnableInserts(enableInserts).setReadSnapshotTimeDifference(readSnapshotTimeDifference)
        .setTableType(tableType);
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

  /** Adds all materialized views defined in the schema to this column family. */
  private void addMaterializedViews() {
    SchemaPlus schema = parentSchema.getSubSchema(name);
    if (schema != null) {
      Iterator<Map.Entry<String, String>> iterator = materializedViewSqls.entrySet().iterator();
      // Do not use enhanced FOR loop here, as it will lead to a
      // ConcurrentModificationException
      // down the stack. https://www.baeldung.com/java-concurrentmodificationexception
      while (iterator.hasNext()) {
        var entry = iterator.next();
        // Add the view for this query
        String viewName = "$" + getTableNames().size();
        CalciteSchema calciteSchema = CalciteSchema.from(schema);

        List<String> viewPath = calciteSchema.path(viewName);

        schema.add(viewName,
            MaterializedViewTable.create(calciteSchema, entry.getValue(), null, viewPath, entry.getKey(), true));
      }
    }
    // Close the hook use to get us here
    addMaterializationsHook.close();
  }

  @SuppressWarnings("deprecation")
  private Hook.Closeable prepareAddMaterializationsHook() {
    return Hook.TRIMMED.add(node -> {
      KuduSchema.this.addMaterializedViews();
    });
  }

  public void assignHints(Holder<SqlToRelConverter.Config> configHolder) {
    configHolder.accept(config -> config.withHintStrategyTable(KuduQuery.KUDU_HINT_STRATEGY_TABLE)
        .withInSubQueryThreshold(Integer.MAX_VALUE));
  }

}
