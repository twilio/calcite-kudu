package com.twilio.raas.sql;

import com.google.common.base.Joiner;
import com.twilio.dataset.DatasetUtil;

import java.util.List;
import java.util.stream.Collectors;

public class JDBCUtil {

  static {
    try {
      // ensure that KuduDriver is registered with DriverManager
      Class.forName(KuduDriver.class.getName());
    } catch (ClassNotFoundException e) {
    }
  }

  public static final String DESCENDING_COLUMNS = getDescendingColumns();

  // TODO see if we can use KuduSchemaFactory by default in KuduDriver
  public static String CALCITE_MODEL_TEMPLATE = "jdbc:kudu:model=inline:{version: '1.0'," +
    "defaultSchema:'kudu',schemas:[{name: 'kudu',type:'custom'," +
    "factory:'com.twilio.raas.sql.KuduSchemaFactory',operand:{connect:'%s'," + DESCENDING_COLUMNS +
    "}]};caseSensitive=false;timeZone=UTC";

  private static String getDescendingColumns() {
    StringBuilder sb = new StringBuilder("kuduTableConfigs:[");
    List <String> descendingList = DatasetUtil.INSTANCE.getDescendingDateFields()
      .stream()
      .map(
        descendingInfo -> String.format("{tableName: '%s', descendingSortedFields:['%s']}",
          descendingInfo.tableName, descendingInfo.columnName))
      .collect(Collectors.toList());
    sb.append(Joiner.on(", ").join(descendingList));
    sb.append("]}");
    return sb.toString();
  }

}
