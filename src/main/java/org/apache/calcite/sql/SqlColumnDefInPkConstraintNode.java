package org.apache.calcite.sql;

import com.twilio.raas.sql.parser.SortOrder;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlColumnDefInPkConstraintNode extends AbstractSqlNode {

  public final SqlIdentifier columnName;
  public final SortOrder sortOrder;

  public SqlColumnDefInPkConstraintNode(
    SqlParserPos pos,
    SqlIdentifier columnName,
    SortOrder sortOrder) {
    super(pos);
    this.columnName = columnName;
    this.sortOrder = sortOrder;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    columnName.unparse(writer, 0, 0);
    if (sortOrder == SortOrder.DESC) {
      writer.print(" ");
      writer.keyword(sortOrder.name());
    }
  }

  @Override
  public boolean equalsDeep(SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlColumnDefInPkConstraintNode)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlColumnDefInPkConstraintNode that = (SqlColumnDefInPkConstraintNode) node;
    if (!this.columnName.equalsDeep(that.columnName, litmus) || this.sortOrder != that.sortOrder) {
      return litmus.fail("{} != {}", this, node);
    }
    return litmus.succeed();
  }

  @Override
  public SqlNode clone(SqlParserPos pos) {
    return new SqlColumnDefInPkConstraintNode(pos, columnName, sortOrder);
  }


}
