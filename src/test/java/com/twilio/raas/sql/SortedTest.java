package com.twilio.raas.sql;

import org.apache.calcite.linq4j.AbstractEnumerable2;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class SortedTest {

    @Test
    public void sortAcrossMultiple() {
        Schema rowSchema = new Schema(Arrays.asList(
                new ColumnSchema.ColumnSchemaBuilder("id", Type.INT64).key(true).build(),
                new ColumnSchema.ColumnSchemaBuilder("id2", Type.INT64).key(true).build()));
        final Object [] smallestRow = {Long.valueOf(1), Long.valueOf(5)};
        final Object[] middleRow = {Long.valueOf(1), Long.valueOf(15)};
        final Object[] largestRow = {Long.valueOf(100), Long.valueOf(20)};

        final List<CalciteRow> lastScanner = Arrays.asList(
            new CalciteRow(rowSchema, smallestRow, Arrays.asList(0), Collections.<Integer>emptyList()),
            new CalciteRow(rowSchema, middleRow, Arrays.asList(0), Collections.<Integer>emptyList())
        );
        final List<CalciteRow> firstScanner = Arrays.asList(
            new CalciteRow(rowSchema, largestRow, Arrays.asList(0), Collections.<Integer>emptyList())
        );

        final List<Enumerator<CalciteRow>> subEnumerables = Arrays.asList(
            Collections.<CalciteRow>emptyList(), firstScanner,
            Collections.<CalciteRow>emptyList(), lastScanner,
            Collections.<CalciteRow>emptyList())
            .stream()
            .map(l -> {
                    return new AbstractEnumerable2<CalciteRow>() {
                        @Override
                        public Iterator<CalciteRow> iterator() {
                            return l.iterator();
                        }
                    };
                })
            .map(e -> e.enumerator())
            .collect(Collectors.toList());

       final Enumerator<Object[]> results = new SortableEnumerable(
           Collections.emptyList(),
           new AtomicBoolean(),
           rowSchema,
           rowSchema,
           -1,
           -1,
            false,
           Collections.<Integer>emptyList())
           .sortedEnumerator(subEnumerables);

        assertTrue("Should have at least one result",
            results.moveNext());
        assertArrayEquals("Should return the smallest record in the enumerables",
            smallestRow, results.current());

        assertTrue("Should have second result",
            results.moveNext());
        assertArrayEquals("Should return the largest record in the enumerables",
            middleRow, results.current());

        assertTrue("Should have third result",
            results.moveNext());
        assertArrayEquals("Should return the largest record in the enumerables",
            largestRow, results.current());

        assertFalse("Should have no more results",
            results.moveNext());
        assertArrayEquals("Should return still largest record -- even though it is done",
            largestRow, results.current());
    }

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
