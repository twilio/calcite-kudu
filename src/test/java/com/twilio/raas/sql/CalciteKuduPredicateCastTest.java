package com.twilio.raas.sql;

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
 * Tests to ensure {@link CalciteKuduPredicate} casts the literal values according to the Kudu Schema
 */
public final class CalciteKuduPredicateCastTest {
  @Test
  public void integerToBigDecimal() {
    final CalciteKuduPredicate predicate = new ComparisonPredicate(0,
        KuduPredicate.ComparisonOp.EQUAL, Integer.valueOf(7));
    final ColumnSchema columnSchema = new ColumnSchemaBuilder("amount", Type.DECIMAL)
      .typeAttributes(
          new ColumnTypeAttributes.ColumnTypeAttributesBuilder().scale(6).precision(22)
          .build())
      .build();
    final KuduPredicate rpcPredicate = predicate.toPredicate(columnSchema ,false);
    final BigDecimal expectedLiteral = BigDecimal.valueOf(7L);
    assertEquals(
        "Kudu Predicate didn't match expected predicate",
        KuduPredicate.newComparisonPredicate(columnSchema, ComparisonOp.EQUAL, expectedLiteral),
        rpcPredicate
    );
  }

  @Test
  public void longToBigDecimal() {
    final CalciteKuduPredicate predicate = new ComparisonPredicate(0,
        KuduPredicate.ComparisonOp.EQUAL, Long.valueOf(7));
    final ColumnSchema columnSchema = new ColumnSchemaBuilder("amount", Type.DECIMAL)
      .typeAttributes(
          new ColumnTypeAttributes.ColumnTypeAttributesBuilder().scale(6).precision(22)
          .build())
      .build();
    final KuduPredicate rpcPredicate = predicate.toPredicate(columnSchema ,false);
    final BigDecimal expectedLiteral = BigDecimal.valueOf(7L);
    assertEquals(
        "Kudu Predicate didn't match expected predicate",
        KuduPredicate.newComparisonPredicate(columnSchema, ComparisonOp.EQUAL, expectedLiteral),
        rpcPredicate
    );
  }
}
