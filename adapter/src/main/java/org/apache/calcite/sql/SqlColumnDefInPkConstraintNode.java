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

import com.twilio.kudu.sql.parser.SortOrder;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;

public class SqlColumnDefInPkConstraintNode extends AbstractSqlNode {

  public final SqlIdentifier columnName;
  public final SortOrder sortOrder;

  public SqlColumnDefInPkConstraintNode(SqlParserPos pos, SqlIdentifier columnName, SortOrder sortOrder) {
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
