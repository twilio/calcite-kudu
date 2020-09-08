package com.twilio.raas.sql;

import com.twilio.raas.sql.parser.KuduSqlParserImpl;
import com.twilio.raas.sql.schema.KuduSchemaFactory;
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

  // TODO see if there is way to set the factory and parserFactory options in KuduDriver
  private static String CONNECTION_PROPS = ";" +
    CalciteConnectionProperty.PARSER_FACTORY + "=" + KuduSqlParserImpl.class.getName() + "#FACTORY";

  public static String CALCITE_TEST_MODEL_TEMPLATE = "jdbc:kudu:model=inline:{version: '1.0'," +
    "defaultSchema:'kudu',schemas:[{name: 'kudu',type:'custom',factory:'%s'," +
    "operand:{connect:'%s', enableInserts:'true'}}]};caseSensitive=false;timeZone=UTC" +
    CONNECTION_PROPS;

  // This enables INSERT support which automatically maintains cube tables when a fact table
  // is written to. It should only be used for testing from a single process as we maintain
  // state on the client to compute the aggregated rows, which is not correct if a table is being
  // written to from multiple processes
  // This also enables DDL support which allows used to create tables
  public static String CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED = "jdbc:kudu:model=inline:{version: '1.0'," +
    "defaultSchema:'kudu',schemas:[{name: 'kudu',type:'custom',factory:'"
    + KuduSchemaFactory.class.getName() + "',operand:{connect:'%s', enableInserts:'true'}}]};" +
    "caseSensitive=false;timeZone=UTC" + CONNECTION_PROPS;

  public static String CALCITE_MODEL_TEMPLATE = "jdbc:kudu:model=inline:{version: '1.0'," +
    "defaultSchema:'kudu',schemas:[{name: 'kudu',type:'custom',factory:'"
    + KuduSchemaFactory.class.getName() + "',operand:{connect:'%s'}}]};caseSensitive=false;timeZone=UTC";

}
