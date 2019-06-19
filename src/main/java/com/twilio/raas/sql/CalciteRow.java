package com.twilio.raas.sql;

import org.apache.kudu.Schema;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.ColumnSchema;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.math.BigDecimal;

/**
 * A Plain Java Object that represents a Projected response from Kudu RPCs. It
 * is {@link Comparable} to itself and plays a role in preserving the natural
 * sort on scans.
 */
public final class CalciteRow implements Comparable<CalciteRow> {
    public final Schema rowSchema;
    public final Object[] rowData;
    public final List<Integer> primaryKeyColumnsInProjection;

    /**
     * Return the Integer indices in the Row Projection that match the primary
     * key columns and in the order they need to match. This lays out how to
     * compare two {@code CalciteRow}s and determine which one is smaller.
     *
     * As an example, imagine we have a table with primary column in order of
     * A, B and we have a scanner SELECT D, C, E, B, A the projectedSchema will
     * be D, C, E, B, A and the tableSchema will be A, B, C, D, E *this*
     * function will return List(4, 3) -- the position's of A and B within the
     * projection and in the order they need to be sorted by.
     */
    public static List<Integer> findPrimaryKeyColumnsInProjection(final Schema projectedSchema, final Schema tableSchema ) {
        final List<Integer> primaryKeyColumnsInProjection = new ArrayList<>();
        final List<ColumnSchema> columnSchemas = projectedSchema.getColumns();

        for (ColumnSchema primaryColumnSchema: tableSchema.getPrimaryKeyColumns()) {
            boolean found = false;
            for (int columnIdx = 0; columnIdx < projectedSchema.getColumnCount(); columnIdx++) {
                if (columnSchemas.get(columnIdx).getName().equals(primaryColumnSchema.getName())) {
                    primaryKeyColumnsInProjection.add(columnIdx);
                    found = true;
                    break;
                }
            }
            // If it isn't found, this means the *next* primary key is not
            // present in the projection. We keep the existing primary keys
            // that were present in the projection in our list. The list is in
            // order -- the order in which it will be sorted.
            if (!found) {
                break;
            }
        }
        return primaryKeyColumnsInProjection;
    }

    /**
     * Create a Calcite row with provided rowData. Used for Testing.
     *
     * @param rowSchema The schema of the query projection
     * @param rowData   Raw data for the row. Needs to conform to rowSchema.
     * @param primaryKeyColumnsInProjection  Ordered list of primary keys within the Projection.
     */
    public CalciteRow(final Schema rowSchema, final Object[] rowData, final List<Integer> primaryKeyColumnsInProjection) {
        this.rowSchema = rowSchema;
        this.rowData = rowData;
        this.primaryKeyColumnsInProjection = primaryKeyColumnsInProjection;
    }

    /**
     * Create a Calcite row from an Scanner result.
     *
     * @param rowFromKudu Row returned from the Scanner RPC.
     * @param primaryKeyColumnsInProjection  Ordered list of primary keys within the Projection.
     */
    public CalciteRow(final RowResult rowFromKudu, final List<Integer> primaryKeyColumnsInProjection) {

        final int rowCount = rowFromKudu.getColumnProjection().getColumns().size();
        this.rowData = new Object[rowCount];
        this.rowSchema = rowFromKudu.getSchema();

        this.primaryKeyColumnsInProjection = primaryKeyColumnsInProjection;

        int columnIndex = 0;
        for (ColumnSchema columnType: this.rowSchema.getColumns()) {
            if (rowFromKudu.isNull(columnIndex)) {
                this.rowData[columnIndex] = null;
            }
            else {
                switch(columnType.getType()) {
                case INT8:
                    this.rowData[columnIndex] = rowFromKudu.getByte(columnIndex);
                    break;
                case INT16:
                    this.rowData[columnIndex] = rowFromKudu.getShort(columnIndex);
                    break;
                case INT32:
                    this.rowData[columnIndex] = rowFromKudu.getInt(columnIndex);
                    break;
                case INT64:
                    this.rowData[columnIndex] = rowFromKudu.getLong(columnIndex);
                    break;
                case STRING:
                    this.rowData[columnIndex] = rowFromKudu.getString(columnIndex);
                    break;
                case BOOL:
                    this.rowData[columnIndex] = rowFromKudu.getBoolean(columnIndex);
                    break;
                case FLOAT:
                    this.rowData[columnIndex] = rowFromKudu.getFloat(columnIndex);
                    break;
                case DOUBLE:
                    this.rowData[columnIndex] = rowFromKudu.getDouble(columnIndex);
                    break;
                case UNIXTIME_MICROS:
                    // @TODO: is this the right response type?
                    this.rowData[columnIndex] = rowFromKudu.getTimestamp(columnIndex).toInstant().toEpochMilli();
                    break;
                case DECIMAL:
                    this.rowData[columnIndex] = rowFromKudu.getDecimal(columnIndex);
                    break;
                default:
                    final ByteBuffer byteBuffer = rowFromKudu.getBinary(columnIndex);
                    byteBuffer.rewind();
                    byte[] returnedValue = new byte[byteBuffer.remaining()];
                    byteBuffer.get(returnedValue);
                    this.rowData[columnIndex] = returnedValue;
                    break;
                }
            }
            columnIndex++;
        }
    }

    @Override
    public int compareTo(CalciteRow o) {
        if (!this.primaryKeyColumnsInProjection.equals(
                o.primaryKeyColumnsInProjection)) {
            throw new RuntimeException("Comparing to Calcite rows that do not have the same primary keys");
        }
        for (Integer positionInProjection: this.primaryKeyColumnsInProjection) {
            final ColumnSchema primaryColumnSchema = this.rowSchema.getColumns().get(positionInProjection);
            switch(primaryColumnSchema.getType()) {
            case INT8:
                if (((Byte)this.rowData[positionInProjection]).compareTo(
                        ((Byte)o.rowData[positionInProjection])) > 0) {
                    return 1;
                }
                break;
            case INT16:
                if (((Short)this.rowData[positionInProjection]).compareTo(
                        ((Short)o.rowData[positionInProjection])) > 0) {
                    return 1;
                }
                break;
            case INT32:
                if (((Integer)this.rowData[positionInProjection]).compareTo(
                        ((Integer)o.rowData[positionInProjection])) > 0) {
                    return 1;
                }
                break;
            case INT64:
                if (((Long)this.rowData[positionInProjection]).compareTo(
                        ((Long)o.rowData[positionInProjection])) > 0) {
                    return 1;
                }
                break;
            case STRING:
                if (((String)this.rowData[positionInProjection]).compareTo(
                        ((String)o.rowData[positionInProjection])) > 0) {
                    return 1;
                }
                break;
            case BOOL:
                if (((Boolean)this.rowData[positionInProjection]).compareTo(
                        ((Boolean)o.rowData[positionInProjection])) > 0) {
                    return 1;
                }
                break;
            case FLOAT:
                if (((Float)this.rowData[positionInProjection]).compareTo(
                        ((Float)o.rowData[positionInProjection])) > 0) {
                    return 1;
                }
                break;
            case DOUBLE:
                if (((Double)this.rowData[positionInProjection]).compareTo(
                        ((Double)o.rowData[positionInProjection])) > 0) {
                    return 1;
                }
                break;
            case UNIXTIME_MICROS:
                // @TODO: is this the right response type?
                if (((Long)this.rowData[positionInProjection]).compareTo(
                        ((Long)o.rowData[positionInProjection])) > 0) {
                    return 1;
                }
                break;
            case DECIMAL:
                if (((BigDecimal)this.rowData[positionInProjection]).compareTo(
                        ((BigDecimal)o.rowData[positionInProjection])) > 0) {
                    return 1;
                }
                break;
            default:
                // Can't compare the others.
                // Should just be Binary.
                break;
            }
        }
        return -1;
    }
}
