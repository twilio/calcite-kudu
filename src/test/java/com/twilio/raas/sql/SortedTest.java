package com.twilio.raas.sql;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;


import static org.junit.Assert.assertEquals;

public final class SortedTest {

    @Test
    public void findPrimaryKeyOrder() {
        final ColumnSchema accountIdColumn = new ColumnSchema.ColumnSchemaBuilder("account_id", Type.INT64).key(true).build();
        final ColumnSchema dateColumn = new ColumnSchema.ColumnSchemaBuilder("date", Type.UNIXTIME_MICROS).key(true).build();
        final ColumnSchema foreignKey = new ColumnSchema.ColumnSchemaBuilder("key_to_other_table", Type.STRING).build();
        final Schema tableSchema = new Schema(Arrays.asList(
                accountIdColumn,
                dateColumn,
                new ColumnSchema.ColumnSchemaBuilder("event_type", Type.STRING).key(true).build(),
                foreignKey));

        assertEquals("Expected to find just account_id from projection",
            Arrays.asList(1), CalciteRow.findPrimaryKeyColumnsInProjection(
                new Schema(Arrays.asList(
                        foreignKey,
                        accountIdColumn)),
                tableSchema));

        assertEquals("Expected to find account_id and date from projection",
            Arrays.asList(2, 1), CalciteRow.findPrimaryKeyColumnsInProjection(
                new Schema(Arrays.asList(
                        foreignKey,
                        dateColumn,
                        accountIdColumn)),
                tableSchema));

        assertEquals("Expected Empty list as it doesn't contain account sid in projection",
            Collections.<Integer>emptyList(), CalciteRow.findPrimaryKeyColumnsInProjection(
                new Schema(Arrays.asList(
                        foreignKey,
                        dateColumn)),
                tableSchema));
    }
}
