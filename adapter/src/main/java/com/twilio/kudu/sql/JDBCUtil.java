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

import com.twilio.kudu.sql.parser.KuduSqlParserImpl;
import com.twilio.kudu.sql.schema.DefaultKuduSchemaFactory;
import com.twilio.kudu.sql.schema.KuduSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.KuduDriver;

public class JDBCUtil {

  static {
    try {
      // ensure that KuduDriver is registered with DriverManager
      Class.forName(KuduDriver.class.getName());
    } catch (ClassNotFoundException e) {
    }
  }

  // This enables INSERT support which automatically maintains cube tables when a
  // fact table is written to. It should only be used for testing from a single
  // process as we maintain state on the client to compute the aggregated rows,
  // which is not correct if a table is being written to from multiple processes.
  // This also enables DDL support which allows used to create tables.
  public static String CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED = "jdbc:kudu:"
      + CalciteConnectionProperty.SCHEMA_FACTORY.camelName() + "=%s" + ";"
      + CalciteConnectionProperty.SCHEMA.camelName() + "=kudu" + ";" + CalciteConnectionProperty.TIME_ZONE.camelName()
      + "=UTC" + ";" + CalciteConnectionProperty.CASE_SENSITIVE.camelName() + "=false" + ";"
      + CalciteConnectionProperty.PARSER_FACTORY.camelName() + "=" + KuduSqlParserImpl.class.getName() + "#FACTORY"
      + ";schema." + KuduSchema.KUDU_CONNECTION_STRING + "=%s" + ";schema." + KuduSchema.ENABLE_INSERTS_FLAG + "=true";

  public static String CALCITE_MODEL_TEMPLATE = "jdbc:kudu:" + CalciteConnectionProperty.SCHEMA_FACTORY.camelName()
      + "=%s" + ";" + CalciteConnectionProperty.SCHEMA.camelName() + "=kudu" + ";"
      + CalciteConnectionProperty.TIME_ZONE.camelName() + "=UTC" + ";"
      + CalciteConnectionProperty.CASE_SENSITIVE.camelName() + "=false" + ";schema." + KuduSchema.KUDU_CONNECTION_STRING
      + "=%s";

}
