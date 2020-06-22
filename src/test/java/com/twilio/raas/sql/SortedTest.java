package com.twilio.raas.sql;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.junit.Ignore;
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
            Arrays.asList(1), CalciteKuduTable.getPrimaryKeyColumnsInProjection(
                tableSchema,
                new Schema(Arrays.asList(
                        foreignKey,
                        accountIdColumn))
                ));

        assertEquals("Expected to find account_id and date from projection",
            Arrays.asList(2, 1), CalciteKuduTable.getPrimaryKeyColumnsInProjection(
                tableSchema,
                new Schema(Arrays.asList(
                        foreignKey,
                        dateColumn,
                        accountIdColumn))
                ));

        assertEquals("Expected to find dateColumn from projection",
            Arrays.asList(1), CalciteKuduTable.getPrimaryKeyColumnsInProjection(
                tableSchema,
                new Schema(Arrays.asList(
                        foreignKey,
                        dateColumn))
                ));
    }
}
