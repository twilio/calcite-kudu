package com.twilio.raas.sql;

public class JDBCUtil {

  public static final String DESCENDING_COLUMNS = "kuduTableConfigs:[" +
      "{tableName: 'ReportCenter.AuditEvents', descendingSortedFields:['event_date']}, " +
      "{tableName: 'AuditEvents-DailyIndex-Aggregation', descendingSortedFields:['event_date']}," +
      "{tableName: 'ReportCenter.MsgInsights', descendingSortedFields:['date_created']}, " +
      "{tableName: 'MsgInsights-Carrier-Aggregation', descendingSortedFields:['date_created']}, " +
      "{tableName: 'MsgInsights-Number-Aggregation', descendingSortedFields:['date_created']}" +
      "]}";

  public static String CALCITE_MODEL_TEMPLATE = "jdbc:calcite:model=inline:{version: '1.0'," +
    "defaultSchema:'kudu',schemas:[{name: 'kudu',type:'custom'," +
    "factory:'com.twilio.raas.sql.KuduSchemaFactory',operand:{connect:'%s'," + DESCENDING_COLUMNS +
    "}]};caseSensitive=false;timeZone=UTC";

}
