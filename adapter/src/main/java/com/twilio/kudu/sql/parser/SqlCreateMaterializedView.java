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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code CREATE MATERIALIZED VIEW} statement.
 */
public class SqlCreateMaterializedView extends SqlCall {
  public final SqlOperator operator;
  public final SqlIdentifier cubeName;
  public final boolean ifNotExists;
  public final SqlSelect query;
  public final SqlNodeList orderByList;

  /** Creates a SqlCreateView. */
  SqlCreateMaterializedView(SqlParserPos pos, SqlIdentifier cubeName, boolean ifNotExists, SqlSelect query,
      SqlNodeList orderByList) {
    super(pos);
    this.operator = new SqlDdlOperator("CREATE MATERIALIZED VIEW", SqlKind.CREATE_MATERIALIZED_VIEW);
    this.cubeName = Objects.requireNonNull(cubeName);
    this.ifNotExists = ifNotExists;
    this.query = Objects.requireNonNull(query);
    this.orderByList = orderByList;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(cubeName, query);
  }

  public SqlOperator getOperator() {
    return operator;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    writer.keyword("MATERIALIZED VIEW");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    cubeName.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AS");
    writer.newlineAndIndent();
    query.unparse(writer, 0, 0);
  }
}
