/* Copyright 2020 Twilio, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twilio.kudu.sql.parser;

import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlColumnDefInPkConstraintNode;
import org.apache.calcite.sql.SqlColumnDefNode;
import org.apache.calcite.sql.SqlColumnNameNode;
import org.apache.calcite.sql.SqlDataTypeNode;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOptionNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class SqlNodeTest {

  private final SqlNode sqlNode;

  public SqlNodeTest(SqlNode sqlNode) {
    this.sqlNode = sqlNode;
  }

  @Parameterized.Parameters(name = "SqlNodeTest_SqlNode={0}") // name is used by
  // failsafe as file name in reports
  public static synchronized Collection<SqlNode> data() {
    SqlParserPos pos = new SqlParserPos(1, 1);
    SqlIdentifier columnName = new SqlIdentifier("COLUMN", pos);
    SqlColumnDefInPkConstraintNode sqlColumnDefInPkConstraintNode = new SqlColumnDefInPkConstraintNode(pos, columnName,
        null);
    SqlOptionNode sqlOptionNode = new SqlOptionNode(pos, "key", "value");
    SqlDataTypeNode sqlDataTypeNode = new SqlDataTypeNode(pos, new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, pos));
    SqlColumnDefNode sqlColumnDefNode = new SqlColumnDefNode(pos, columnName, sqlDataTypeNode, true, false, false,
        SortOrder.ASC, null, null, null, -1, null);
    SqlColumnNameNode sqlColumnNameNode = new SqlColumnNameNode(pos, columnName);
    return Arrays.asList(sqlOptionNode, sqlColumnDefInPkConstraintNode, sqlColumnDefNode, sqlColumnNameNode,
        sqlDataTypeNode);
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
