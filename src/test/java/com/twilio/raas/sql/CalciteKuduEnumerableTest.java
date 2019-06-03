package com.twilio.raas.sql;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import com.twilio.raas.sql.CalciteScannerMessage.MessageType;
import org.junit.Test;
import org.apache.calcite.linq4j.Enumerator;

import static org.junit.Assert.*;

public final class CalciteKuduEnumerableTest {
    @Test
    public void noResults() {
        final LinkedBlockingQueue<CalciteScannerMessage<Object[]>> queue = new LinkedBlockingQueue<>(10);
        final Enumerator<Object[]> enumerable = new CalciteKuduEnumerable(queue, 2, -1, new AtomicBoolean(false))
            .enumerator();
        queue.add(CalciteScannerMessage.<Object[]>createEndMessage());
        queue.add(CalciteScannerMessage.<Object[]>createEndMessage());
        assertFalse("Should not be any new messages",
            enumerable.moveNext());
        // This used to Hang forever looping on the poll call.
        assertFalse("Should respond with no move next after first query",
            enumerable.moveNext());
    }
    @Test
    public void oneResult() {
        final LinkedBlockingQueue<CalciteScannerMessage<Object[]>> queue = new LinkedBlockingQueue<>(10);
        final Enumerator<Object[]> enumerable = new CalciteKuduEnumerable(queue, 1, -1, new AtomicBoolean(false))
            .enumerator();
        final Object[] singleRow = {Long.valueOf(1)};
        queue.add(new CalciteScannerMessage<Object[]>(singleRow));
        queue.add(CalciteScannerMessage.<Object[]>createEndMessage());
        assertTrue("Should signal there are messages",
            enumerable.moveNext());
        assertArrayEquals("Row should match",
            singleRow, enumerable.current());
        assertFalse("Should be no more rows",
            enumerable.moveNext());
        assertArrayEquals("current() should still be the previous row",
            singleRow, enumerable.current());
    }
    @Test
    public void limitOfOneResult() {
        final LinkedBlockingQueue<CalciteScannerMessage<Object[]>> queue = new LinkedBlockingQueue<>(10);
        final Enumerator<Object[]> enumerable = new CalciteKuduEnumerable(queue, 2, 1, new AtomicBoolean(false))
            .enumerator();
        final Object[] singleRow = {Long.valueOf(1)};
        final Object[] secondRow = {Long.valueOf(1)};
        queue.add(new CalciteScannerMessage<Object[]>(singleRow));
        queue.add(new CalciteScannerMessage<Object[]>(secondRow));
        queue.add(CalciteScannerMessage.<Object[]>createEndMessage());
        queue.add(CalciteScannerMessage.<Object[]>createEndMessage());
        assertTrue("Should signal there are messages",
            enumerable.moveNext());
        assertArrayEquals("Row should match",
            singleRow, enumerable.current());
        assertFalse("Should be no more rows",
            enumerable.moveNext());
    }

    @Test
    public void twoScannersOneWithNoResults() {
        final LinkedBlockingQueue<CalciteScannerMessage<Object[]>> queue = new LinkedBlockingQueue<>(10);
        final Enumerator<Object[]> enumerable = new CalciteKuduEnumerable(queue, 2, -1, new AtomicBoolean(false))
            .enumerator();
        final Object[] singleRow = {Long.valueOf(1)};
        queue.add(CalciteScannerMessage.<Object[]>createEndMessage());
        queue.add(new CalciteScannerMessage<Object[]>(singleRow));
        queue.add(CalciteScannerMessage.<Object[]>createEndMessage());
        assertTrue("Should signal there are messages",
            enumerable.moveNext());
        assertArrayEquals("Row should match",
            singleRow, enumerable.current());
        assertFalse("Should be no more rows",
            enumerable.moveNext());
    }
}
