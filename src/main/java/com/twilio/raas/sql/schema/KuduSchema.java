package com.twilio.raas.sql.schema;

import com.twilio.kudu.metadata.CubeTableInfo;
import com.twilio.kudu.metadata.KuduTableMetadata;
import com.twilio.raas.sql.CalciteKuduTable;
import com.twilio.raas.sql.CalciteKuduTableBuilder;
import com.twilio.raas.sql.CalciteModifiableKuduTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduTable;
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

    private boolean enableInserts;

    public KuduSchema(final String connectString,
                      final Map<String, KuduTableMetadata> kuduTableMetadataMap,
                      final String enableInsertsString) {
        this.client = new AsyncKuduClient.AsyncKuduClientBuilder(connectString).build();
        this.kuduTableMetadataMap = kuduTableMetadataMap;
        // We disable inserts by default as this feaure is meant for testing purposes
        this.enableInserts = ( enableInsertsString!=null) ? Boolean.valueOf(enableInsertsString)
          : false;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (cachedTableMap.isPresent()) {
            return cachedTableMap.get();
        }

        HashMap<String, Table> tableMap = new HashMap<>();
        final List<String> tableNames;
        try {
            tableNames = this.client
                .getTablesList()
                .join()
                .getTablesList();
        }
        catch (Exception threadInterrupted) {
            return Collections.emptyMap();
        }

        // create CalciteKuduTables
        for (Map.Entry<String, KuduTableMetadata> entry : kuduTableMetadataMap.entrySet()) {
          KuduTableMetadata kuduTableMetadata = entry.getValue();
          final List<String> descendingOrderedColumnNames =
            kuduTableMetadata.getDescendingOrderedColumnNames();

          // create any cube tables first
          List<CalciteKuduTable> cubeTableList = new ArrayList<>(5);
          for (CubeTableInfo cubeTableInfo : kuduTableMetadata.getCubeTableInfo()) {
            Optional<KuduTable> cubeTableOptional = openKuduTable(cubeTableInfo.tableName);
            cubeTableOptional.ifPresent(kuduTable -> {
                final CalciteKuduTableBuilder builder = new CalciteKuduTableBuilder(kuduTable,
                  client, enableInserts)
                  .setTableType(com.twilio.raas.sql.TableType.CUBE)
                  .setEventTimeAggregationType(cubeTableInfo.eventTimeAggregationType);
                setDescendingFieldIndices(builder, descendingOrderedColumnNames, kuduTable);
                setTimestampColumnIndex(builder, kuduTableMetadata.getTimestampColumnName(),
                  kuduTable);
                CalciteKuduTable calciteKuduTable = builder.build();
                tableMap.put(cubeTableInfo.tableName, calciteKuduTable);
                cubeTableList.add(calciteKuduTable);
              }
            );
          }

          // create the fact table
          String factTableName = entry.getKey();
          Optional<KuduTable> factTableOptional = openKuduTable(factTableName);
          factTableOptional.ifPresent( kuduTable -> {
            final CalciteKuduTableBuilder builder = new CalciteKuduTableBuilder(kuduTable, client
              , enableInserts)
              .setTableType(com.twilio.raas.sql.TableType.FACT)
              .setCubeTables(cubeTableList);
            setDescendingFieldIndices(builder, descendingOrderedColumnNames, kuduTable);
            setTimestampColumnIndex(builder, kuduTableMetadata.getTimestampColumnName(),
              kuduTable);
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
        for (String tableName: tableNames) {
          if (!tableMap.containsKey(tableName)) {
            if (tableName.startsWith("System")) {
              Optional<KuduTable> kuduTableOptional = openKuduTable(tableName);
              kuduTableOptional.ifPresent( kuduTable ->  {
                createCalciteTable(tableMap, kuduTable, com.twilio.raas.sql.TableType.SYSTEM);
              });
            }
            else {
              Optional<KuduTable> kuduTableOptional = openKuduTable(tableName);
              kuduTableOptional.ifPresent( kuduTable ->  {
                createCalciteTable(tableMap, kuduTable, com.twilio.raas.sql.TableType.DIMENSION);
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
                                  com.twilio.raas.sql.TableType tableType) {
    final CalciteKuduTableBuilder builder = new CalciteKuduTableBuilder(kuduTable, client, enableInserts)
      .setTableType(tableType);
    CalciteKuduTable calciteKuduTable = builder.build();
    tableMap.put(kuduTable.getName(), calciteKuduTable);
  }

  private Optional<KuduTable> openKuduTable(String tableName) {
    try {
      return Optional.of(client.openTable(tableName).join());
    }
    catch (Exception e) {
      logger.error("Unable to open table " + tableName, e);
      return Optional.empty();
    }
  }

  private void setDescendingFieldIndices(CalciteKuduTableBuilder builder,
                                         List<String> descendingOrderedColumnNames, KuduTable kuduTable) {
    final List<Integer> descendingOrderedColumnIndices =
      descendingOrderedColumnNames.stream()
        .map(name -> kuduTable.getSchema().getColumnIndex(name))
        .collect(Collectors.toList());
    builder.setDescendingOrderedFieldIndices(descendingOrderedColumnIndices);
  }

  private void setTimestampColumnIndex(CalciteKuduTableBuilder builder,
                                       String timestampColumnName, KuduTable kuduTable) {
      if (timestampColumnName!=null) {
        builder.setTimestampColumnIndex(kuduTable.getSchema().getColumnIndex(timestampColumnName));
      }
  }

}
