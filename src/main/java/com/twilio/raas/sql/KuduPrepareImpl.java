package com.twilio.raas.sql;

import com.google.common.collect.ImmutableMap;
import com.twilio.raas.sql.parser.SqlCreateTable;
import com.twilio.raas.sql.schema.KuduSchema;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlColumnDefInPkConstraintNode;
import org.apache.calcite.sql.SqlColumnDefNode;
import org.apache.calcite.sql.SqlColumnNameNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOptionNode;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class KuduPrepareImpl extends CalcitePrepareImpl {

  @Override
  public void executeDdl(Context context, SqlNode node) {
    final KuduSchema kuduSchema = getKuduSchema(context.getRootSchema().plus());
    final KuduClient kuduClient = kuduSchema.getClient().syncClient();
    switch (node.getKind()) {
      case CREATE_TABLE:
        SqlCreateTable createTableNode = (SqlCreateTable) node;

        //validate at most one ROW_TIMESTAMP column is defined
        List<String> rowTimestampColumns =
          StreamSupport.stream(createTableNode.columnDefs.spliterator(), false)
          .filter(columnDefNode -> ((SqlColumnDefNode) columnDefNode).isRowTimestamp)
          .map(columnDefNode -> ((SqlColumnDefNode) columnDefNode).columnName.getSimple())
          .collect(Collectors.toList());
        if (rowTimestampColumns.size() > 1) {
          throw new IllegalArgumentException("Only one ROW_TIMESTAMP column can be defined found "
            + rowTimestampColumns);
        }


        // pk columns names from the PRIMARY KEY constraint
        List<String> pkConstraintColumns = StreamSupport
          .stream(createTableNode.pkConstraintColumnDefs.spliterator(), false)
          .map(columnDefNode -> ((SqlColumnDefInPkConstraintNode) columnDefNode).columnName.getSimple())
          .collect(Collectors.toList());

        // get the column schemas from the column definition nodes
        List<ColumnSchema> columnSchemas =
          StreamSupport.stream(createTableNode.columnDefs.spliterator(), false)
            .map(columnDefNode -> {
              ColumnSchema.ColumnSchemaBuilder builder =
                ((SqlColumnDefNode)columnDefNode).columnSchemaBuilder;
              if (pkConstraintColumns.contains(((SqlColumnDefNode) columnDefNode).columnName.getSimple())) {
                builder.key(true);
                builder.nullable(false);
              }
              return builder.build();
            })
            .collect(Collectors.toList());

        if (!pkConstraintColumns.isEmpty()) {
          // order the column schemas so that the order of primary key columns matches that of
          // defined in the primary key constraint
          Collections.sort(columnSchemas, (cs1, cs2) -> {
            int cs1PKIndex = pkConstraintColumns.indexOf(cs1.getName());
            int cs2PKIndex = pkConstraintColumns.indexOf(cs2.getName());
            // if the column isn't a PK column place it at the end of the list
            cs1PKIndex = cs1PKIndex ==-1 ? Integer.MAX_VALUE : cs1PKIndex;
            cs2PKIndex = cs2PKIndex ==-1 ? Integer.MAX_VALUE : cs2PKIndex;
            return Integer.compare(cs1PKIndex, cs2PKIndex);
          });
        }
        final Schema tableSchema = new Schema(columnSchemas);

        // set the hash partitions
        final org.apache.kudu.client.CreateTableOptions createTableOptions =
          new org.apache.kudu.client.CreateTableOptions();
        if (!SqlNodeList.isEmptyList(createTableNode.hashPartitionColumns)) {
          List<String> hashPartitionColumns = StreamSupport.stream(createTableNode.hashPartitionColumns.spliterator(), false)
            .map(columnNameNode -> ((SqlColumnNameNode) columnNameNode).getColumnName().toString())
            .collect(Collectors.toList());
          createTableOptions.addHashPartitions(hashPartitionColumns, createTableNode.hashBuckets);
        }

        // if there is a row timestamp column define create a single dummy range partition for
        // that column so that we can add new partitions later
        if (!rowTimestampColumns.isEmpty()) {
          String rowTimestampColumn = rowTimestampColumns.get(0);
          PartialRow lowerBound = tableSchema.newPartialRow();
          lowerBound.addTimestamp(rowTimestampColumn, new Timestamp(Long.MIN_VALUE));
          PartialRow upperBound = tableSchema.newPartialRow();
          upperBound.addTimestamp(rowTimestampColumn, new Timestamp(Long.MIN_VALUE+1));
          createTableOptions.addRangePartition(lowerBound, upperBound);
        }

        if (createTableNode.numReplicas != -1) {
          createTableOptions.setNumReplicas(createTableNode.numReplicas);
        }

        // set extra configs
        if (!createTableNode.tableOptions.equals(SqlNodeList.EMPTY)) {
          ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder();
          StreamSupport.stream(createTableNode.tableOptions.spliterator(), false)
            .forEach( option -> {
              SqlOptionNode optionNode = (SqlOptionNode)option;
              builder.put(optionNode.propertyName, optionNode.value);
            });
          createTableOptions.setExtraConfigs(builder.build());
        }

        // return if the table already exists
        try {
          if (createTableNode.ifNotExists && kuduClient.tableExists(createTableNode.tableName.toString())) {
            // we don't check if the table schema matches that of the create table ddl statement
            return;
          }
        } catch (KuduException e) {
          throw new RuntimeException(e);
        }

        // create the table
        try {
          kuduClient.createTable(createTableNode.tableName.toString(), tableSchema, createTableOptions);
          kuduSchema.clearCachedTableMap();
        } catch (KuduException e) {
          throw new RuntimeException(e);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported DDL operation " + node.getKind() +
          " " + node.getClass());
    }
  }

  public static KuduSchema getKuduSchema(SchemaPlus rootSchema) {
    for (String subSchemaName : rootSchema.getSubSchemaNames()) {
      try {
        KuduSchema kuduSchema = rootSchema.getSubSchema(subSchemaName).unwrap(KuduSchema.class);
        return kuduSchema;
      } catch (ClassCastException e) {
      }
    }
    throw new RuntimeException("Unable to find KuduSchema in " + rootSchema);
  }

}
