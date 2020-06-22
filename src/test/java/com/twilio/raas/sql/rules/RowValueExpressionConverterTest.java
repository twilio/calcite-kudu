package com.twilio.raas.sql.rules;

import com.twilio.raas.sql.CalciteKuduTable;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowValueExpressionConverterTest {
  @Test
  public void testRVC() {
    final RexBuilder builder = new RexBuilder(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));

    final RexLiteral val1 = builder.makeLiteral("a");
    final RexLiteral val2 = builder.makeLiteral("b");
    final RexLiteral val3 = builder.makeLiteral("c");

    // create 3 columns with indexes 0, 1, 2
    final RexInputRef col1 = builder.makeInputRef(new BasicSqlType(
      builder.getTypeFactory().getTypeSystem(), SqlTypeName.VARCHAR), 0);
    final RexInputRef col2 = builder.makeInputRef(new BasicSqlType(
      builder.getTypeFactory().getTypeSystem(), SqlTypeName.VARCHAR), 1);
    final RexInputRef col3 = builder.makeInputRef(new BasicSqlType(
      builder.getTypeFactory().getTypeSystem(), SqlTypeName.VARCHAR), 2);

    // ROW(col0, col1, col2)
    final RexCall rvc1 = (RexCall) builder.makeCall(
      SqlStdOperatorTable.ROW,
      Arrays.asList(col1, col2, col3));

    // ROW('a', 'b', 'c')
    final RexCall rvc2 = (RexCall) builder.makeCall(
      SqlStdOperatorTable.ROW,
      Arrays.asList(val1, val2, val3));

    // ROW(col0, col1, col2) > ROW('a', 'b', 'c')
    final RexCall call = (RexCall) builder.makeCall(
      SqlStdOperatorTable.GREATER_THAN,
      Arrays.asList(rvc1, rvc2));

    // col0 > 'a'
    RexCall exp1 = createExp1(builder, col1, val1, SqlStdOperatorTable.GREATER_THAN);

    // (col0 = 'a' OR col1 > 'b')
    RexCall exp2 = createExp2(builder, col1, val1, col2, val2,
      SqlStdOperatorTable.GREATER_THAN);

    // (col0 = 'a' OR col2 = 'b' OR col2 > 'b)
    RexCall exp3 = createExp3(builder, col1, val1, col2, val2, col3, val3,
      SqlStdOperatorTable.GREATER_THAN);

    RexCall orAndExps = (RexCall) builder.makeCall(SqlStdOperatorTable.OR,
      Arrays.asList(exp1, exp2, exp3));

    CalciteKuduTable kuduTable = mock(CalciteKuduTable.class);
    RowValueExpressionConverter converter = new RowValueExpressionConverter(builder, kuduTable);
    Assert.assertEquals("Row value expression was not converted correctly",
      orAndExps, converter.visitCall(call));

    // test with DESC ordered columns (0,2)

    // col0 < 'a'
    exp1 = createExp1(builder, col1, val1, SqlStdOperatorTable.LESS_THAN);

    // (col0 = 'a' OR col1 > 'b')
    exp2 = createExp2(builder, col1, val1, col2, val2,
      SqlStdOperatorTable.GREATER_THAN);

    // (col0 = 'a' OR col2 = 'b' OR col2 < 'b)
    exp3 = createExp3(builder, col1, val1, col2, val2, col3, val3,
      SqlStdOperatorTable.LESS_THAN);

    orAndExps = (RexCall) builder.makeCall(SqlStdOperatorTable.OR,
      Arrays.asList(exp1, exp2, exp3));

    when(kuduTable.isColumnOrderedDesc(0)).thenReturn(true);
    when(kuduTable.isColumnOrderedDesc(2)).thenReturn(true);
    converter = new RowValueExpressionConverter(builder, kuduTable);
    Assert.assertEquals("Row value expression was not converted correctly",
      orAndExps, converter.visitCall(call));
  }

  private RexCall createExp3(RexBuilder builder, RexInputRef col1, RexLiteral val1,
                             RexInputRef col2, RexLiteral val2, RexInputRef col3, RexLiteral val3
    , SqlBinaryOperator op) {
    final RexCall eq1 = (RexCall) builder.makeCall(SqlStdOperatorTable.EQUALS,
      Arrays.asList(col1, val1));
    final RexCall eq2 = (RexCall) builder.makeCall(SqlStdOperatorTable.EQUALS,
      Arrays.asList(col2, val2));
    final RexCall gt3 = (RexCall) builder.makeCall(op,
      Arrays.asList(col3, val3));
    final RexCall and3 = (RexCall) builder.makeCall(SqlStdOperatorTable.AND,
      Arrays.asList(eq1, eq2, gt3));
    return and3;
  }

  private RexCall createExp2(RexBuilder builder, RexInputRef col1, RexLiteral val1,
                             RexInputRef col2, RexLiteral val2, SqlBinaryOperator op) {
    final RexCall eq1 = (RexCall) builder.makeCall(SqlStdOperatorTable.EQUALS,
      Arrays.asList(col1, val1));
    final RexCall gt2 = (RexCall) builder.makeCall(op,
      Arrays.asList(col2, val2));
    return (RexCall) builder.makeCall(SqlStdOperatorTable.AND, Arrays.asList(eq1, gt2));
  }

  private RexCall createExp1(RexBuilder builder, RexInputRef col1, RexLiteral val1,
                             SqlBinaryOperator op) {
    return (RexCall) builder.makeCall(op, Arrays.asList(col1, val1));
  }
}