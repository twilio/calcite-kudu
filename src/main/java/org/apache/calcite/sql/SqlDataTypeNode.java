package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

import java.util.Objects;

public class SqlDataTypeNode extends AbstractSqlNode {
  public final String typeName;
  public final Integer precision;
  public final Integer scale;

  public SqlDataTypeNode(SqlParserPos pos, SqlTypeNameSpec typeNameSpec) {
    super(pos);
    if (typeNameSpec.getTypeName().isSimple()) {
      this.typeName = typeNameSpec.getTypeName().getSimple();
    } else {
      throw new RuntimeException("Invalid data type name: " + typeNameSpec);
    }
    if (this.typeName.equals("DECIMAL")) {
      SqlBasicTypeNameSpec basicTypeNameSpec = (SqlBasicTypeNameSpec)typeNameSpec;
      if (basicTypeNameSpec.getScale() == -1) {
        throw new IllegalStateException("Scale must be set if data type is DECIMAL");
      }
      if (basicTypeNameSpec.getPrecision() == -1) {
        throw new IllegalStateException("Precision must be set if data type is DECIMAL");
      }
      scale = basicTypeNameSpec.getScale();
      precision = basicTypeNameSpec.getPrecision();
    }
    else {
      scale = null;
      precision = null;
    }
  }

  public SqlDataTypeNode(SqlParserPos pos, String typeName, Integer precision, Integer scale) {
    super(pos);
    this.typeName = typeName;
    this.scale = scale;
    this.precision = precision;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(typeName);
    if (scale != null && precision != null) {
      writer.print("(");
      writer.print(precision);
      writer.print(",");
      writer.print(scale);
      writer.print(")");
    }
  }

  @Override
  public boolean equalsDeep(SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlDataTypeNode)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlDataTypeNode that = (SqlDataTypeNode) node;
    if (!this.typeName.equals(that.typeName) ||
        !Objects.equals(this.precision, that.precision) ||
        !Objects.equals(this.scale, that.scale)) {
      return litmus.fail("{} != {}", this, node);
    }
    return litmus.succeed();
  }

  @Override
  public SqlNode clone(SqlParserPos pos) {
    return new SqlDataTypeNode(pos, typeName, precision, scale);
  }

}
