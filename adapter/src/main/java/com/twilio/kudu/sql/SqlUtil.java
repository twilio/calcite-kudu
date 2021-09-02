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
package com.twilio.kudu.sql;

import org.apache.calcite.sql.SqlDataTypeNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.NlsString;
import org.apache.kudu.Type;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlUtil {
  public static String getExplainPlan(ResultSet rs) throws SQLException {
    StringBuilder buf = new StringBuilder();
    while (rs.next()) {
      buf.append(rs.getString(1));
      buf.append('\n');
    }
    if (buf.length() > 0) {
      buf.setLength(buf.length() - 1);
    }
    return buf.toString();
  }

  public static List<List<Object>> getResult(ResultSet rs) throws SQLException {
    int numCols = rs.getMetaData().getColumnCount();
    List<List<Object>> rows = new ArrayList<>();
    while (rs.next()) {
      List<Object> row = new ArrayList<>(numCols);
      for (int i=1; i<=numCols; ++i) {
        row.add(rs.getObject(i));
      }
      rows.add(row);
    }
    return rows;
  }

  /**
   * Translates JDBC sql types to Kudu data types
   *
   * @param dataType the datatype from Calcite
   * @return Kudu type that represents this Calcite type
   */
  public static Type getKuduDataType(SqlDataTypeNode dataType) {
    Type kuduType = null;
    int jdbcType = SqlTypeName.valueOf(dataType.typeName).getJdbcOrdinal();
    switch (jdbcType) {
    case Types.BOOLEAN:
      kuduType = Type.BOOL;
      break;
    case Types.TINYINT:
      kuduType = Type.INT8;
      break;
    case Types.SMALLINT:
      kuduType = Type.INT16;
      break;
    case Types.INTEGER:
      kuduType = Type.INT32;
      break;
    case Types.BIGINT:
      kuduType = Type.INT64;
      break;
    case Types.DECIMAL:
      kuduType = Type.DECIMAL;
      break;
    case Types.FLOAT:
      kuduType = Type.FLOAT;
      break;
    case Types.DOUBLE:
      kuduType = Type.DOUBLE;
      break;
    case Types.VARBINARY:
      kuduType = Type.BINARY;
      break;
    case Types.VARCHAR:
      kuduType = Type.STRING;
      break;
    case Types.TIMESTAMP:
      kuduType = Type.UNIXTIME_MICROS;
      break;
    default:
      throw new UnsupportedOperationException("Datatype is not supported " + dataType);
    }
    return kuduType;
  }

  /**
   * Translates the default object value type to the data type that is expected by
   * Kudu
   *
   * @param kuduType     Kudu type in the table
   * @param defaultValue default value taken from {@link org.apache.kudu.Schema}
   * @return Translated defaultValue into java type Calcite expects
   */
  public static Object translateDefaultValue(Type kuduType, Object defaultValue) {
    switch (kuduType) {
    case STRING:
    case VARCHAR:
      if (defaultValue instanceof NlsString) {
        return ((NlsString) defaultValue).getValue();
      }
      break;
    case FLOAT:
      if (defaultValue instanceof BigDecimal) {
        return ((BigDecimal) defaultValue).floatValue();
      }
      break;
    case DOUBLE:
      if (defaultValue instanceof BigDecimal) {
        return ((BigDecimal) defaultValue).doubleValue();
      }
      break;
    case DECIMAL:
      if (defaultValue instanceof BigDecimal) {
        return defaultValue;
      }
      break;
    case INT8:
      if (defaultValue instanceof BigDecimal) {
        return ((BigDecimal) defaultValue).byteValueExact();
      }
      break;
    case INT16:
      if (defaultValue instanceof BigDecimal) {
        return ((BigDecimal) defaultValue).shortValueExact();
      }
      break;
    case INT32:
      if (defaultValue instanceof BigDecimal) {
        return ((BigDecimal) defaultValue).intValueExact();
      }
      break;
    case UNIXTIME_MICROS:
    case INT64:
      if (defaultValue instanceof BigDecimal) {
        return ((BigDecimal) defaultValue).longValue();
      }
      break;
    case BINARY:
      if (defaultValue instanceof BitString) {
        return ((BitString) defaultValue).getAsByteArray();
      }
      break;
    case BOOL:
      if (defaultValue instanceof Boolean) {
        return (Boolean) defaultValue;
      }
    case DATE:
      // @TODO: parse the string using a date formatter and return:
      // 32-bit days since the Unix epoch
      break;
    }
    throw new UnsupportedOperationException("Type " + defaultValue.getClass() + " of " + "value " + defaultValue
        + " is not a valid default value for kudu type " + kuduType);

  }
}
