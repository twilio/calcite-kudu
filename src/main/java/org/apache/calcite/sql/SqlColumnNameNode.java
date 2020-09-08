package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlColumnNameNode  extends AbstractSqlNode {

  private final SqlIdentifier columnName;

  public SqlColumnNameNode(
    SqlParserPos pos,
    SqlIdentifier columnName) {
    super(pos);
    this.columnName = columnName;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.print(columnName.getSimple());
  }

  @Override
  public boolean equalsDeep(SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlColumnNameNode)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlColumnNameNode that = (SqlColumnNameNode) node;
    if (!this.columnName.equalsDeep(that.columnName, litmus)) {
      return litmus.fail("{} != {}", this, node);
    }
    return litmus.succeed();
  }

  @Override
  public SqlNode clone(SqlParserPos pos) {
    return new SqlColumnNameNode(pos, columnName);
  }

  public SqlIdentifier getColumnName() {
    return columnName;
  }

}
