package com.twilio.raas.sql;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import com.twilio.raas.sql.CalciteScannerMessage.MessageType;
import org.junit.Before;
import org.junit.Test;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.kudu.Schema;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import java.util.Arrays;

import static org.junit.Assert.*;

public final class CalciteKuduEnumerableTest {
    private Schema rowSchema;

    @Before
    public void setupRowSchema() {
        rowSchema = new Schema(Arrays.asList(
                new ColumnSchema.ColumnSchemaBuilder("id", Type.INT64).key(true).build()));
    }

    @Test
    public void noResults() {
        final LinkedBlockingQueue<CalciteScannerMessage<CalciteRow>> queue = new LinkedBlockingQueue<>(10);
        final Enumerator<CalciteRow> enumerable = new CalciteKuduEnumerable(queue, new AtomicBoolean(false))
            .enumerator();
        queue.add(CalciteScannerMessage.<CalciteRow>createEndMessage());
        queue.add(CalciteScannerMessage.<CalciteRow>createEndMessage());
        assertFalse("Should not be any new messages",
            enumerable.moveNext());
        // This used to Hang forever looping on the poll call.
        assertFalse("Should respond with no move next after first query",
            enumerable.moveNext());
    }
    @Test
    public void oneResult() {
        final LinkedBlockingQueue<CalciteScannerMessage<CalciteRow>> queue = new LinkedBlockingQueue<>(10);
        final Enumerator<CalciteRow> enumerable = new CalciteKuduEnumerable(queue, new AtomicBoolean(false))
            .enumerator();
        final Object[] singleRow = {Long.valueOf(1)};
        queue.add(new CalciteScannerMessage<CalciteRow>(new CalciteRow(rowSchema, singleRow, Arrays.asList(0), Collections.<Integer>emptyList())));
        queue.add(CalciteScannerMessage.<CalciteRow>createEndMessage());
        assertTrue("Should signal there are messages",
            enumerable.moveNext());
        assertEquals("Row should match",
            singleRow[0], enumerable.current().getRowData());
        assertFalse("Should be no more rows",
            enumerable.moveNext());
        assertEquals("current() should still be the previous row",
            singleRow[0], enumerable.current().getRowData());
    }
}
