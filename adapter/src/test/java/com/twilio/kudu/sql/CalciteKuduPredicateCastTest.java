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

import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Type;
import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduPredicate.ComparisonOp;
import org.junit.Test;

/**
 * Tests to ensure {@link CalciteKuduPredicate} casts the literal values
 * according to the Kudu Schema
 */
public final class CalciteKuduPredicateCastTest {
  @Test
  public void integerToBigDecimal() {
    final CalciteKuduPredicate predicate = new ComparisonPredicate(0, KuduPredicate.ComparisonOp.EQUAL,
        Integer.valueOf(7));
    final ColumnSchema columnSchema = new ColumnSchemaBuilder("amount", Type.DECIMAL)
        .typeAttributes(new ColumnTypeAttributes.ColumnTypeAttributesBuilder().scale(6).precision(22).build()).build();
    final KuduPredicate rpcPredicate = predicate.toPredicate(columnSchema, false);
    final BigDecimal expectedLiteral = BigDecimal.valueOf(7L);
    assertEquals("Kudu Predicate didn't match expected predicate",
        KuduPredicate.newComparisonPredicate(columnSchema, ComparisonOp.EQUAL, expectedLiteral), rpcPredicate);
  }

  @Test
  public void longToBigDecimal() {
    final CalciteKuduPredicate predicate = new ComparisonPredicate(0, KuduPredicate.ComparisonOp.EQUAL,
        Long.valueOf(7));
    final ColumnSchema columnSchema = new ColumnSchemaBuilder("amount", Type.DECIMAL)
        .typeAttributes(new ColumnTypeAttributes.ColumnTypeAttributesBuilder().scale(6).precision(22).build()).build();
    final KuduPredicate rpcPredicate = predicate.toPredicate(columnSchema, false);
    final BigDecimal expectedLiteral = BigDecimal.valueOf(7L);
    assertEquals("Kudu Predicate didn't match expected predicate",
        KuduPredicate.newComparisonPredicate(columnSchema, ComparisonOp.EQUAL, expectedLiteral), rpcPredicate);
  }
}
