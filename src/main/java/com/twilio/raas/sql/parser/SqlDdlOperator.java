package com.twilio.raas.sql.parser;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;

/**
 * Operator for a DDL statement. Used by as th operator for {@link SqlCreateTable}
 * The unparse method must be implemented while overriding a SqlSpecialOperator as the base class
 * implementation calls getSyntax().unparse() which throws an exception.
 */
public class SqlDdlOperator extends SqlSpecialOperator {
  public SqlDdlOperator(String name, SqlKind kind) {
    super(name, kind);
  }

  @Override
  public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    call.unparse(writer, leftPrec, rightPrec);
  }

  @Override
  protected void unparseListClause(SqlWriter writer, SqlNode clause) {
    super.unparseListClause(writer, clause);
  }
}
