package com.twilio.raas.sql;

import org.apache.kudu.Schema;
import org.apache.kudu.ColumnSchema;
import java.util.List;
import java.math.BigDecimal;

/**
 * A Plain Java Object that represents a Projected response from Kudu RPCs. It
 * is {@link Comparable} to itself and plays a role in preserving the natural
 * sort on scans.
 */
public final class CalciteRow implements Comparable<CalciteRow> {
    private final Object[] rowData;
    public final Schema rowSchema;
    public final List<Integer> primaryKeyColumnsInProjection;
    public final List<Integer> descendingSortedFieldIndices;

    /**
     * Create a Calcite row with provided rowData. Used for Testing.
     *
     * @param rowSchema The schema of the query projection
     * @param rowData   Raw data for the row. Needs to conform to rowSchema.
     * @param primaryKeyColumnsInProjection  Ordered list of primary keys within the Projection.
     * @param descendingSortedFieldIndices  Index of the descending sorted fields in the rowSchema projection
     */
    public CalciteRow(final Schema rowSchema,
                      final Object[] rowData,
                      final List<Integer> primaryKeyColumnsInProjection,
                      final List<Integer> descendingSortedFieldIndices) {
        this.rowSchema = rowSchema;
        this.rowData = rowData;
        this.primaryKeyColumnsInProjection = primaryKeyColumnsInProjection;
        this.descendingSortedFieldIndices = descendingSortedFieldIndices;
    }

    @Override
    public int compareTo(CalciteRow o) {
        if (!this.primaryKeyColumnsInProjection.equals(
                o.primaryKeyColumnsInProjection)) {
            throw new RuntimeException("Comparing two Calcite rows that do not have the same " +
                    "primary keys");
        }
        for (Integer positionInProjection: this.primaryKeyColumnsInProjection) {
            final ColumnSchema primaryColumnSchema = this.rowSchema.getColumns().get(positionInProjection);
            int cmp = 0;
            switch(primaryColumnSchema.getType()) {
            case INT8:
                cmp = ((Byte) this.rowData[positionInProjection]).compareTo(
                        ((Byte) o.rowData[positionInProjection]));
                if (cmp != 0) {
                    return (descendingSortedFieldIndices.contains(positionInProjection)) ? Math.negateExact(cmp) : cmp;
                }
                break;
            case INT16:
                cmp = ((Short) this.rowData[positionInProjection]).compareTo(
                        ((Short) o.rowData[positionInProjection]));
                if (cmp != 0) {
                    return (descendingSortedFieldIndices.contains(positionInProjection)) ? Math.negateExact(cmp) : cmp;
                }
                break;
            case INT32:
                cmp = ((Integer) this.rowData[positionInProjection]).compareTo(
                        ((Integer) o.rowData[positionInProjection]));
                if (cmp != 0) {
                    return (descendingSortedFieldIndices.contains(positionInProjection)) ? Math.negateExact(cmp) : cmp;
                }
                break;
            // @TODO: is this the right response type?
            case UNIXTIME_MICROS:
            case INT64:
                cmp = ((Long) this.rowData[positionInProjection]).compareTo(
                        ((Long) o.rowData[positionInProjection]));
                if (cmp != 0) {
                    // Negate comparator sign based on if column is descending sorted
                    return (descendingSortedFieldIndices.contains(positionInProjection)) ? Math.negateExact(cmp) : cmp;
                }
                break;
            case STRING:
                cmp = ((String) this.rowData[positionInProjection]).compareTo(
                        ((String) o.rowData[positionInProjection]));
                if (cmp != 0) {
                    return cmp;
                }
                break;
            case BOOL:
                if (((Boolean)this.rowData[positionInProjection]).compareTo(
                        ((Boolean)o.rowData[positionInProjection])) != 0) {
                    return 1;
                }
                break;
            case FLOAT:
                cmp = ((Float) this.rowData[positionInProjection]).compareTo(
                        ((Float) o.rowData[positionInProjection]));
                if (cmp != 0) {
                    return cmp;
                }
                break;
            case DOUBLE:
                cmp = ((Double) this.rowData[positionInProjection]).compareTo(
                        ((Double) o.rowData[positionInProjection]));
                if (cmp > 0) {
                    return cmp;
                }
                break;
            case DECIMAL:
                cmp = ((BigDecimal) this.rowData[positionInProjection]).compareTo(
                        ((BigDecimal) o.rowData[positionInProjection]));
                if (cmp != 0) {
                    return cmp;
                }
                break;
            default:
                // Can't compare the others.
                throw new RuntimeException("Cannot compare column " + primaryColumnSchema.getName()
                        + " of type "+ primaryColumnSchema.getType());
            }
        }
        return 0;
    }

    public Object getRowData() {
        return rowData.length==1 ? rowData[0] : rowData;
    }
}
