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

import com.twilio.kudu.sql.SqlUtil;
import com.twilio.kudu.sql.parser.SortOrder;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Litmus;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Type;

public class SqlColumnDefNode extends AbstractSqlNode {

  public final SqlIdentifier columnName;
  public final SqlDataTypeNode dataType;
  public final boolean isNullable;
  public final SortOrder sortOrder;
  public final boolean isPk;
  public final boolean isRowTimestamp;
  public final SqlNode defaultValueExp;
  public final SqlNode encoding;
  public final SqlNode compression;
  public final int blockSize;
  public final SqlNode comment;

  public final ColumnSchema.ColumnSchemaBuilder columnSchemaBuilder;

  public SqlColumnDefNode(SqlParserPos pos, SqlIdentifier columnName, SqlDataTypeNode dataType, boolean isNullable,
      boolean isPk, boolean isRowTimestamp, SortOrder sortOrder, SqlNode defaultValueExp, SqlNode encoding,
      SqlNode compression, int blockSize, SqlNode comment) {
    super(pos);
    this.columnName = columnName;
    this.dataType = dataType;
    this.isNullable = isNullable;
    this.isPk = isPk;
    this.isRowTimestamp = isRowTimestamp;
    this.sortOrder = sortOrder;
    this.defaultValueExp = defaultValueExp;
    this.encoding = encoding;
    this.compression = compression;
    this.blockSize = blockSize;
    this.comment = comment;

    Type kuduDataType = SqlUtil.getKuduDataType(dataType);
    columnSchemaBuilder = new ColumnSchema.ColumnSchemaBuilder(columnName.getSimple(), kuduDataType).key(isPk)
        // if the column is not a PK set the nullability
        .nullable(!isPk && isNullable).desiredBlockSize(blockSize);
    // for DECIMAL types set the scale and precision
    if (kuduDataType == Type.DECIMAL) {
      ColumnTypeAttributes scaleAndPrecision = new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
          .scale(dataType.scale).precision(dataType.precision).build();
      columnSchemaBuilder.typeAttributes(scaleAndPrecision);
    }
    // set the default value
    if (defaultValueExp != null) {
      if (!(defaultValueExp instanceof SqlLiteral)) {
        throw new IllegalArgumentException("Default value should be a literal constant");
      }
      Object defaultValue = ((SqlLiteral) defaultValueExp).getValue();
      columnSchemaBuilder.defaultValue(SqlUtil.translateDefaultValue(kuduDataType, defaultValue));
    }
    // set the encoding
    if (encoding != null) {
      if (!(encoding instanceof SqlLiteral)) {
        throw new IllegalArgumentException("Encoding should be a literal constant");
      }
      String encodingString = ((SqlLiteral) encoding).getStringValue();
      columnSchemaBuilder.encoding(ColumnSchema.Encoding.valueOf(encodingString));
    }
    // set the compression
    if (compression != null) {
      if (!(compression instanceof SqlLiteral)) {
        throw new IllegalArgumentException("Encoding should be a literal constant");
      }
      String compressionString = ((SqlLiteral) compression).getStringValue();
      columnSchemaBuilder.compressionAlgorithm(ColumnSchema.CompressionAlgorithm.valueOf(compressionString));
    }
    if (blockSize != -1) {
      columnSchemaBuilder.desiredBlockSize(blockSize);
    }
    // set the comment
    if (comment != null) {
      if (!(comment instanceof SqlLiteral)) {
        throw new IllegalArgumentException("Encoding should be a literal constant");
      }
      String commentString = ((SqlLiteral) comment).getStringValue();
      columnSchemaBuilder.comment(commentString);
    }
  }

  @Override
  public SqlNode clone(SqlParserPos pos) {
    return new SqlColumnDefNode(pos, columnName, dataType, isNullable, isPk, isRowTimestamp, sortOrder, defaultValueExp,
        encoding, compression, blockSize, comment);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    columnName.unparse(writer, 0, 0);
    writer.print(" ");
    dataType.unparse(writer, 0, 0);
    if (isPk) {
      writer.keyword(" PRIMARY KEY");
      if (sortOrder == SortOrder.DESC) {
        writer.keyword(" DESC");
      }
    }
    if (!isNullable) {
      writer.keyword(" NOT NULL");
    }
    if (encoding != null) {
      writer.keyword(" COLUMN_ENCODING ");
      encoding.unparse(writer, 0, 0);
    }
    if (compression != null) {
      writer.keyword(" COMPRESSION ");
      compression.unparse(writer, 0, 0);
    }
    if (defaultValueExp != null) {
      writer.keyword(" DEFAULT ");
      defaultValueExp.unparse(writer, 0, 0);
    }
    if (blockSize != -1) {
      writer.keyword(" BLOCK_SIZE ");
      writer.print(blockSize);
    }
    if (isRowTimestamp) {
      writer.keyword(" ROW_TIMESTAMP");
    }
    if (comment != null) {
      writer.keyword(" COMMENT ");
      comment.unparse(writer, 0, 0);
    }
  }

  @Override
  public boolean equalsDeep(SqlNode node, Litmus litmus) {
    if (!(node instanceof SqlColumnDefNode)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlColumnDefNode that = (SqlColumnDefNode) node;
    if (!this.columnName.equalsDeep(that.columnName, litmus) || !this.dataType.equalsDeep(that.dataType, litmus)
        || this.isNullable != that.isNullable || this.isPk != that.isPk || this.sortOrder != that.sortOrder
        || this.isRowTimestamp != that.isRowTimestamp
        || (this.defaultValueExp != null && !this.defaultValueExp.equalsDeep(that.defaultValueExp, litmus))
        || (this.encoding != null && !this.encoding.equalsDeep(that.encoding, litmus))
        || (this.compression != null && !this.compression.equalsDeep(that.compression, litmus))
        || this.blockSize != that.blockSize
        || (this.comment != null && !this.comment.equalsDeep(that.comment, litmus))) {
      return litmus.fail("{} != {}", this, node);
    }
    return litmus.succeed();
  }
}
