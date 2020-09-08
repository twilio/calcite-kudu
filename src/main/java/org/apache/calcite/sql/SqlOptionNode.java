package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlOptionNode extends AbstractSqlNode {
  public final String propertyName;
  public final String value;

  public SqlOptionNode(SqlParserPos pos, SqlNode key, SqlLiteral literal) {
    super(pos);
    this.propertyName = trim(key.toString());
    this.value = literal.toValue();
  }

  public SqlOptionNode(SqlParserPos pos, SqlNode key, SqlIdentifier identifier) {
    super(pos);
    this.propertyName = trim(key.toString());
    this.value = identifier.toString();
  }

  public SqlOptionNode(SqlParserPos pos, String propertyName, String value) {
    super(pos);
    this.propertyName = propertyName;
    this.value = value;
  }

  private String trim(String name) {
    return name.substring(1, name.length()-1);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.print("'");
    writer.print(propertyName);
    writer.print("'");
    writer.print("=");
    writer.print(value);
  }

  @Override
  public boolean equalsDeep(SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlOptionNode)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlOptionNode that = (SqlOptionNode) node;
    if (!this.propertyName.equals(that.propertyName) || !this.value.equals(that.value)) {
      return litmus.fail("{} != {}", this, node);
    }
    return litmus.succeed();
  }

  @Override
  public SqlNode clone(SqlParserPos pos) {
    return new SqlOptionNode(pos, propertyName, value);
  }

}
