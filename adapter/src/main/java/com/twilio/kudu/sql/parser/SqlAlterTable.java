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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlColumnDefNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Parse tree node for SQL {@code ALTER TABLE} command.
 */
public class SqlAlterTable extends SqlCall {
  public final SqlOperator operator;

  public final SqlIdentifier tableName;
  public final SqlNodeList columnDefs;
  public final boolean isAdd;
  public final SqlNodeList columnNames;
  public final boolean ifNotExists;
  public final boolean ifExists;

  /** Creates a ALTER TABLE. */
  public SqlAlterTable(SqlParserPos pos, SqlIdentifier tableName, SqlNodeList columnDefs, boolean isAdd, SqlNodeList columnNames, boolean ifNotExists, boolean ifExists) {
    super(pos);
    this.operator = new SqlDdlOperator("ALTER TABLE", SqlKind.ALTER_TABLE);
    this.tableName = tableName;
    this.columnDefs = columnDefs;
    this.isAdd = isAdd;
    this.columnNames = columnNames;
    this.ifNotExists = ifNotExists;
    this.ifExists = ifExists;
  }

  public SqlOperator getOperator() {
    return operator;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableList.of(tableName, columnDefs, columnNames);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(operator.getName());
    tableName.unparse(writer, 0, 0);
    if(isAdd) {
      writer.keyword("ADD COLUMNS");
      if (ifNotExists) {
        writer.keyword("IF NOT EXISTS");
      }
      writer.print("(");
      ((SqlDdlOperator) getOperator()).unparseListClause(writer, columnDefs);
      writer.print(")");
    }
    else {
      writer.keyword("DROP COLUMNS");
      if (ifExists) {
        writer.keyword("IF EXISTS");
      }
      writer.print("(");
      ((SqlDdlOperator) getOperator()).unparseListClause(writer, columnNames);
      writer.print(")");
    }

  }
}
