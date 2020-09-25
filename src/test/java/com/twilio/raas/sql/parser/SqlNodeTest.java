package com.twilio.raas.sql.parser;

import org.apache.calcite.config.NullCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlColumnDefInPkConstraintNode;
import org.apache.calcite.sql.SqlColumnDefNode;
import org.apache.calcite.sql.SqlColumnNameNode;
import org.apache.calcite.sql.SqlDataTypeNode;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOptionNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlModality;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Litmus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class SqlNodeTest {

  private final SqlNode sqlNode;

  public SqlNodeTest(SqlNode sqlNode) {
    this.sqlNode = sqlNode;
  }

  @Parameterized.Parameters(name="SqlNodeTest_SqlNode={0}") // name is used by
  // failsafe as file name in reports
  public static synchronized Collection<SqlNode> data() {
    SqlParserPos pos = new SqlParserPos(1, 1);
    SqlIdentifier columnName = new SqlIdentifier("COLUMN", pos);
    SqlColumnDefInPkConstraintNode sqlColumnDefInPkConstraintNode =
      new SqlColumnDefInPkConstraintNode(pos , columnName, null);
    SqlOptionNode sqlOptionNode = new SqlOptionNode(pos, "key", "value");
    SqlDataTypeNode sqlDataTypeNode = new SqlDataTypeNode(pos,
      new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, pos));
    SqlColumnDefNode sqlColumnDefNode = new SqlColumnDefNode(pos, columnName, sqlDataTypeNode, true, false, false
      , SortOrder.ASC, null, null, null, -1, null);
    SqlColumnNameNode sqlColumnNameNode = new SqlColumnNameNode(pos, columnName);
    return Arrays.asList(sqlOptionNode, sqlColumnDefInPkConstraintNode, sqlColumnDefNode,
      sqlColumnNameNode, sqlDataTypeNode);
  }

  @Test
  public void testValidate() {
    // create a validator that does nothing
    SqlValidator validator = mock(SqlValidator.class);
    sqlNode.validate(validator, null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testAccept() {
    sqlNode.accept(new SqlShuttle());
  }

  @Test
  public void testEqualsDeep() {
    SqlParserPos pos = new SqlParserPos(1, 1);
    SqlNode hint = new SqlHint(pos, null, null, null);
    assertFalse(sqlNode.equalsDeep(hint, Litmus.IGNORE));
    assertTrue(sqlNode.equalsDeep(sqlNode, Litmus.IGNORE));
  }

}
