package org.apache.kudu.client;
// NOTE: package up here is hack. Gives us access to
// AbstractKuduScannerBuilder.lowerBoundPartitionKeyRaw and
// AbstractKuduScannerBuilder.exclusiveUpperBoundPartitionKeyRaw

import org.apache.kudu.client.AbstractKuduScannerBuilder;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.ReplicaSelection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Common;
import org.apache.kudu.client.Client.ScanTokenPB;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.shaded.com.google.protobuf.CodedInputStream;

public class KuduScannerUtil {

    /**
     * Deserializes a {@code KuduScanToken} into a {@link AsyncKuduScanner}.
     * @param buf a byte array containing the serialized scan token.
     * @param client a async Kudu client for the cluster
     * @param table table that we are scanning against.
     * @return a async scanner for the serialized scan token
     */
    public static AsyncKuduScanner deserializeIntoAsyncScanner(byte[] buf, AsyncKuduClient client, KuduTable table) throws IOException {
        return pbIntoAsyncScanner(ScanTokenPB.parseFrom(CodedInputStream.newInstance(buf)), client, table);
    }

    private static AsyncKuduScanner pbIntoAsyncScanner(ScanTokenPB message,
        AsyncKuduClient client, KuduTable table) {
        Preconditions.checkArgument(
                !message.getFeatureFlagsList().contains(ScanTokenPB.Feature.Unknown),
                "Scan token requires an unsupported feature. This Kudu client must be updated.");

        AsyncKuduScanner.AsyncKuduScannerBuilder builder = client.newScannerBuilder(table);
        populateBuilder(client, message, builder, table);
        return builder.build();
    }

    private static void populateBuilder(AsyncKuduClient client, ScanTokenPB message, AbstractKuduScannerBuilder builder, KuduTable table) {
        List<Integer> columns = new ArrayList<>(message.getProjectedColumnsCount());
        for (Common.ColumnSchemaPB column : message.getProjectedColumnsList()) {
            int columnIdx = table.getSchema().getColumnIndex(column.getName());
            ColumnSchema schema = table.getSchema().getColumnByIndex(columnIdx);
            if (column.getType() != schema.getType().getDataType(schema.getTypeAttributes())) {
                throw new IllegalStateException(String.format(
                        "invalid type %s for column '%s' in scan token, expected: %s",
                        column.getType().name(), column.getName(), schema.getType().name()));
            }
            if (column.getIsNullable() != schema.isNullable()) {
                throw new IllegalStateException(String.format(
                        "invalid nullability for column '%s' in scan token, expected: %s",
                        column.getName(), column.getIsNullable() ? "NULLABLE" : "NOT NULL"));

            }

            columns.add(columnIdx);
        }
        builder.setProjectedColumnIndexes(columns);

        for (Common.ColumnPredicatePB pred : message.getColumnPredicatesList()) {
            builder.addPredicate(KuduPredicate.fromPB(table.getSchema(), pred));
        }

        if (message.hasLowerBoundPrimaryKey()) {
            builder.lowerBoundRaw(message.getLowerBoundPrimaryKey().toByteArray());
        }
        if (message.hasUpperBoundPrimaryKey()) {
            builder.exclusiveUpperBoundRaw(message.getUpperBoundPrimaryKey().toByteArray());
        }

        if (message.hasLowerBoundPartitionKey()) {
            builder.lowerBoundPartitionKeyRaw(message.getLowerBoundPartitionKey().toByteArray());
        }
        if (message.hasUpperBoundPartitionKey()) {
            builder.exclusiveUpperBoundPartitionKeyRaw(message.getUpperBoundPartitionKey().toByteArray());
        }

        if (message.hasLimit()) {
            builder.limit(message.getLimit());
        }

        if (message.hasReadMode()) {
            switch (message.getReadMode()) {
                case READ_AT_SNAPSHOT: {
                    builder.readMode(AsyncKuduScanner.ReadMode.READ_AT_SNAPSHOT);
                    if (message.hasSnapTimestamp()) {
                        builder.snapshotTimestampRaw(message.getSnapTimestamp());
                    }
                    break;
                }
                case READ_LATEST: {
                    builder.readMode(AsyncKuduScanner.ReadMode.READ_LATEST);
                    break;
                }
                case READ_YOUR_WRITES: {
                    builder.readMode(AsyncKuduScanner.ReadMode.READ_YOUR_WRITES);
                    break;
                }
                default: throw new IllegalArgumentException("unknown read mode");
            }
        }

        if (message.hasReplicaSelection()) {
            switch (message.getReplicaSelection()) {
                case LEADER_ONLY: {
                    builder.replicaSelection(ReplicaSelection.LEADER_ONLY);
                    break;
                }
                case CLOSEST_REPLICA: {
                    builder.replicaSelection(ReplicaSelection.CLOSEST_REPLICA);
                    break;
                }
                default: throw new IllegalArgumentException("unknown replica selection policy");
            }
        }

        if (message.hasPropagatedTimestamp() &&
                message.getPropagatedTimestamp() != AsyncKuduClient.NO_TIMESTAMP) {
            client.updateLastPropagatedTimestamp(message.getPropagatedTimestamp());
        }

        if (message.hasCacheBlocks()) {
            builder.cacheBlocks(message.getCacheBlocks());
        }

        if (message.hasFaultTolerant()) {
            builder.setFaultTolerant(message.getFaultTolerant());
        }

        if (message.hasBatchSizeBytes()) {
            builder.batchSizeBytes(message.getBatchSizeBytes());
        }
    }

}
