package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public abstract class AbstractSqlNode extends SqlNode {

  public AbstractSqlNode(SqlParserPos pos) {
    super(pos);
  }

  @Override
  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validate(this);
  }

  @Override
  public <R> R accept(SqlVisitor<R> visitor) {
    throw new UnsupportedOperationException();
  }

}
