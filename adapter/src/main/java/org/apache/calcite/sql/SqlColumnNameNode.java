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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

public class SqlColumnNameNode extends AbstractSqlNode {

  private final SqlIdentifier columnName;

  public SqlColumnNameNode(SqlParserPos pos, SqlIdentifier columnName) {
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
