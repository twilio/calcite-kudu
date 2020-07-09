package com.twilio.raas.sql.rules;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import com.twilio.raas.sql.ComparisonPredicate;

/**
 * This test confirms the behavior of the push down filter parses the numeric literal into a proper
 * push down filter.
 * RC-1221
 */
public final class NumberFilterTest {
  @Test
  public void amountIsPushedDownProperly() throws Exception {
    final RexBuilder builder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
    final RexLiteral amountFilter = builder.makeExactLiteral(new BigDecimal("0.5"));
    final RexInputRef fieldRef = builder.makeInputRef(new BasicSqlType(
            builder.getTypeFactory().getTypeSystem(), SqlTypeName.DECIMAL), 0);

    final RexCall call = (RexCall) builder.makeCall(
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
        Arrays.asList(fieldRef, amountFilter));

    final KuduPredicatePushDownVisitor visitor = new KuduPredicatePushDownVisitor();

    final ComparisonPredicate predicate = (ComparisonPredicate) visitor.visitLiteral(amountFilter, call)
      .get(0)
      .get(0);
    Assert.assertEquals("The amount should match what was passed in",
        new BigDecimal("0.5"),
        predicate.rightHandValue);
  }
}
