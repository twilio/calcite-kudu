/* Copyright 2020 Twilio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twilio.kudu.sql;

import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

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
    rowSchema = new Schema(Arrays.asList(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT64).key(true).build()));
  }

  @Test
  public void noResults() {
    final LinkedBlockingQueue<CalciteScannerMessage<CalciteRow>> queue = new LinkedBlockingQueue<>(10);
    final Enumerator<CalciteRow> enumerable = new CalciteKuduEnumerable(queue, new AtomicBoolean(false)).enumerator();
    queue.add(CalciteScannerMessage.<CalciteRow>createEndMessage());
    queue.add(CalciteScannerMessage.<CalciteRow>createEndMessage());
    assertFalse("Should not be any new messages", enumerable.moveNext());
    // This used to Hang forever looping on the poll call.
    assertFalse("Should respond with no move next after first query", enumerable.moveNext());
  }

  @Test
  public void oneResult() {
    final LinkedBlockingQueue<CalciteScannerMessage<CalciteRow>> queue = new LinkedBlockingQueue<>(10);
    final Enumerator<CalciteRow> enumerable = new CalciteKuduEnumerable(queue, new AtomicBoolean(false)).enumerator();
    final Object[] singleRow = { Long.valueOf(1) };
    queue.add(new CalciteScannerMessage<CalciteRow>(
        new CalciteRow(rowSchema, singleRow, Arrays.asList(0), Collections.<Integer>emptyList())));
    queue.add(CalciteScannerMessage.<CalciteRow>createEndMessage());
    assertTrue("Should signal there are messages", enumerable.moveNext());
    assertEquals("Row should match", singleRow[0], enumerable.current().getRowData());
    assertFalse("Should be no more rows", enumerable.moveNext());
    assertEquals("current() should still be the previous row", singleRow[0], enumerable.current().getRowData());
  }

  @Test
  public void errorResult() {
    final LinkedBlockingQueue<CalciteScannerMessage<CalciteRow>> queue = new LinkedBlockingQueue<>(10);
    final Enumerator<CalciteRow> enumerable = new CalciteKuduEnumerable(queue, new AtomicBoolean(false)).enumerator();
    final Object[] singleRow = { Long.valueOf(1) };
    queue.add(new CalciteScannerMessage<CalciteRow>(
        new CalciteRow(rowSchema, singleRow, Arrays.asList(0), Collections.<Integer>emptyList())));
    queue.add(new CalciteScannerMessage<CalciteRow>(new RuntimeException("Testing exit")));
    assertTrue("Should signal there are messages", enumerable.moveNext());
    assertEquals("Row should match", singleRow[0], enumerable.current().getRowData());
    assertFalse("Should be no more rows", enumerable.moveNext());
    assertEquals("current() should still be the previous row", singleRow[0], enumerable.current().getRowData());
  }
}
