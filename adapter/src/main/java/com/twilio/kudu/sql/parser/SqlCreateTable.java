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
 * Parse tree node for SQL {@code CREATE TABLE} command.
 */
public class SqlCreateTable extends SqlCall {
  public final SqlOperator operator;

  public final SqlIdentifier tableName;
  public final boolean ifNotExists;
  public final SqlNodeList columnDefs;
  public final SqlNodeList pkConstraintColumnDefs;
  public final SqlNodeList tableOptions;
  public final SqlNodeList hashPartitionColumns;
  public final int hashBuckets;
  public final int numReplicas;

  /** Creates a CREATE TABLE. */
  public SqlCreateTable(SqlParserPos pos, SqlIdentifier tableName, boolean ifNotExists, SqlNodeList columnDefs,
      SqlNodeList pkConstraintColumnDefs, SqlNodeList tableOptions, SqlNodeList hashPartitionColumns, int hashBuckets,
      int numReplicas) {
    super(pos);
    this.operator = new SqlDdlOperator("CREATE TABLE", SqlKind.CREATE_TABLE);
    this.tableName = tableName;
    this.ifNotExists = ifNotExists;
    this.columnDefs = columnDefs;
    this.pkConstraintColumnDefs = pkConstraintColumnDefs;
    this.tableOptions = tableOptions;
    this.hashPartitionColumns = hashPartitionColumns;
    this.hashBuckets = hashBuckets;
    this.numReplicas = numReplicas;

    boolean hasPKColumnAttribute = !StreamSupport.stream(columnDefs.spliterator(), false)
        .filter(n -> ((SqlColumnDefNode) n).isPk).collect(Collectors.toList()).isEmpty();
    if (!pkConstraintColumnDefs.equals(SqlNodeList.EMPTY) && hasPKColumnAttribute) {
      throw new IllegalArgumentException(
          "Cannot use both PRIMARY KEY column attribute and " + "PRIMARY KEY CONSTRAINT clause");
    }
  }

  public SqlOperator getOperator() {
    return operator;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableList.of(tableName, columnDefs, pkConstraintColumnDefs, tableOptions, hashPartitionColumns);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(operator.getName());
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    tableName.unparse(writer, 0, 0);
    writer.print("(");
    ((SqlDdlOperator) getOperator()).unparseListClause(writer, columnDefs);
    if (pkConstraintColumnDefs != null) {
      writer.keyword(", PRIMARY KEY(");
      ((SqlDdlOperator) getOperator()).unparseListClause(writer, pkConstraintColumnDefs);
      writer.keyword(")");
    }
    writer.print(")");
    if (!SqlNodeList.isEmptyList(hashPartitionColumns)) {
      writer.keyword("PARTITION BY HASH(");
      ((SqlDdlOperator) getOperator()).unparseListClause(writer, hashPartitionColumns);
      writer.keyword(") PARTITIONS ");
      writer.print(hashBuckets);
    }
    if (!SqlNodeList.isEmptyList(tableOptions)) {
      writer.keyword(" TBLPROPERTIES (");
      ((SqlDdlOperator) getOperator()).unparseListClause(writer, tableOptions);
      writer.print(")");
    }
  }
}
