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
    SqlValidator validator = new SqlValidator() {
      @Override
      public SqlConformance getConformance() {
        return null;
      }

      @Override
      public SqlValidatorCatalogReader getCatalogReader() {
        return null;
      }

      @Override
      public SqlOperatorTable getOperatorTable() {
        return null;
      }

      @Override
      public SqlNode validate(SqlNode topNode) {
        return topNode;
      }

      @Override
      public SqlNode validateParameterizedExpression(SqlNode topNode, Map<String, RelDataType> nameToTypeMap) {
        return null;
      }

      @Override
      public void validateQuery(SqlNode node, SqlValidatorScope scope, RelDataType targetRowType) {
      }

      @Override
      public RelDataType getValidatedNodeType(SqlNode node) {
        return null;
      }

      @Override
      public RelDataType getValidatedNodeTypeIfKnown(SqlNode node) {
        return null;
      }

      @Override
      public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope) {
      }

      @Override
      public void validateLiteral(SqlLiteral literal) {
      }

      @Override
      public void validateIntervalQualifier(SqlIntervalQualifier qualifier) {
      }

      @Override
      public void validateInsert(SqlInsert insert) {
      }

      @Override
      public void validateUpdate(SqlUpdate update) {
      }

      @Override
      public void validateDelete(SqlDelete delete) {
      }

      @Override
      public void validateMerge(SqlMerge merge) {
      }

      @Override
      public void validateDataType(SqlDataTypeSpec dataType) {

      }

      @Override
      public void validateDynamicParam(SqlDynamicParam dynamicParam) {

      }

      @Override
      public void validateWindow(SqlNode windowOrId, SqlValidatorScope scope, SqlCall call) {
      }

      @Override
      public void validateMatchRecognize(SqlCall pattern) {
      }

      @Override
      public void validateCall(SqlCall call, SqlValidatorScope scope) {

      }

      @Override
      public void validateAggregateParams(SqlCall aggCall, SqlNode filter, SqlNodeList orderList, SqlValidatorScope scope) {

      }

      @Override
      public void validateColumnListParams(SqlFunction function, List<RelDataType> argTypes, List<SqlNode> operands) {
      }

      @Nullable
      @Override
      public SqlCall makeNullaryCall(SqlIdentifier id) {
        return null;
      }

      @Override
      public RelDataType deriveType(SqlValidatorScope scope, SqlNode operand) {
        return null;
      }

      @Override
      public CalciteContextException newValidationError(SqlNode node, Resources.ExInst<SqlValidatorException> e) {
        return null;
      }

      @Override
      public boolean isAggregate(SqlSelect select) {
        return false;
      }

      @Override
      public boolean isAggregate(SqlNode selectNode) {
        return false;
      }

      @Override
      public SqlWindow resolveWindow(SqlNode windowOrRef, SqlValidatorScope scope) {
        return null;
      }

      @Override
      public SqlValidatorNamespace getNamespace(SqlNode node) {
        return null;
      }

      @Override
      public String deriveAlias(SqlNode node, int ordinal) {
        return null;
      }

      @Override
      public SqlNodeList expandStar(SqlNodeList selectList, SqlSelect query, boolean includeSystemVars) {
        return null;
      }

      @Override
      public SqlValidatorScope getWhereScope(SqlSelect select) {
        return null;
      }

      @Override
      public RelDataTypeFactory getTypeFactory() {
        return null;
      }

      @Override
      public void setValidatedNodeType(SqlNode node, RelDataType type) {

      }

      @Override
      public void removeValidatedNodeType(SqlNode node) {
      }

      @Override
      public RelDataType getUnknownType() {
        return null;
      }

      @Override
      public SqlValidatorScope getSelectScope(SqlSelect select) {
        return null;
      }

      @Override
      public SelectScope getRawSelectScope(SqlSelect select) {
        return null;
      }

      @Override
      public SqlValidatorScope getFromScope(SqlSelect select) {
        return null;
      }

      @Override
      public SqlValidatorScope getJoinScope(SqlNode node) {
        return null;
      }

      @Override
      public SqlValidatorScope getGroupScope(SqlSelect select) {
        return null;
      }

      @Override
      public SqlValidatorScope getHavingScope(SqlSelect select) {
        return null;
      }

      @Override
      public SqlValidatorScope getOrderScope(SqlSelect select) {
        return null;
      }

      @Override
      public SqlValidatorScope getMatchRecognizeScope(SqlMatchRecognize node) {
        return null;
      }

      @Override
      public void declareCursor(SqlSelect select, SqlValidatorScope scope) {

      }

      @Override
      public void pushFunctionCall() {
      }

      @Override
      public void popFunctionCall() {
      }

      @Override
      public String getParentCursor(String columnListParamName) {
        return null;
      }

      @Override
      public void setIdentifierExpansion(boolean expandIdentifiers) {

      }

      @Override
      public void setColumnReferenceExpansion(boolean expandColumnReferences) {
      }

      @Override
      public boolean getColumnReferenceExpansion() {
        return false;
      }

      @Override
      public void setDefaultNullCollation(NullCollation nullCollation) {

      }

      @Override
      public NullCollation getDefaultNullCollation() {
        return null;
      }

      @Override
      public boolean shouldExpandIdentifiers() {
        return false;
      }

      @Override
      public void setCallRewrite(boolean rewriteCalls) {

      }

      @Override
      public RelDataType deriveConstructorType(SqlValidatorScope scope, SqlCall call, SqlFunction unresolvedConstructor, SqlFunction resolvedConstructor, List<RelDataType> argTypes) {
        return null;
      }

      @Override
      public CalciteException handleUnresolvedFunction(SqlCall call, SqlFunction unresolvedFunction, List<RelDataType> argTypes, List<String> argNames) {
        return null;
      }

      @Override
      public SqlNode expandOrderExpr(SqlSelect select, SqlNode orderExpr) {
        return null;
      }

      @Override
      public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
        return null;
      }

      @Override
      public boolean isSystemField(RelDataTypeField field) {
        return false;
      }

      @Override
      public List<List<String>> getFieldOrigins(SqlNode sqlQuery) {
        return null;
      }

      @Override
      public RelDataType getParameterRowType(SqlNode sqlQuery) {
        return null;
      }

      @Override
      public SqlValidatorScope getOverScope(SqlNode node) {
        return null;
      }

      @Override
      public boolean validateModality(SqlSelect select, SqlModality modality, boolean fail) {
        return false;
      }

      @Override
      public void validateWith(SqlWith with, SqlValidatorScope scope) {

      }

      @Override
      public void validateWithItem(SqlWithItem withItem) {
      }

      @Override
      public void validateSequenceValue(SqlValidatorScope scope, SqlIdentifier id) {
      }

      @Override
      public SqlValidatorScope getWithScope(SqlNode withItem) {
        return null;
      }

      @Override
      public SqlValidator setLenientOperatorLookup(boolean lenient) {
        return null;
      }

      @Override
      public boolean isLenientOperatorLookup() {
        return false;
      }

      @Override
      public SqlValidator setEnableTypeCoercion(boolean enabled) {
        return null;
      }

      @Override
      public boolean isTypeCoercionEnabled() {
        return false;
      }

      @Override
      public void setTypeCoercion(TypeCoercion typeCoercion) {
      }

      @Override
      public TypeCoercion getTypeCoercion() {
        return null;
      }

      @Override
      public void setSqlTypeCoercionRules(SqlTypeCoercionRule typeCoercionRules) {
      }

      @Override
      public Config config() {
        return null;
      }

      @Override
      public SqlValidator transform(UnaryOperator<Config> transform) {
        return null;
      }
    };
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
