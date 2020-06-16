package com.twilio.raas.sql;

import com.twilio.kudu.metadata.CubeTableInfo;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduTable;

import java.util.Collections;
import java.util.List;

public class CalciteKuduTableBuilder {
  private KuduTable kuduTable;
  private AsyncKuduClient client;
  private List<Integer> descendingOrderedFieldIndices = Collections.emptyList();

  private Integer timestampColumnIndex = -1;
  private List<CalciteKuduTable> cubeTabes = Collections.emptyList();
  private TableType tableType = TableType.FACT;

  private CubeTableInfo.EventTimeAggregationType eventTimeAggregationType = null;

  private final boolean enableInserts;

  public CalciteKuduTableBuilder(KuduTable kuduTable, AsyncKuduClient client,
                                 boolean enableInserts) {
    this.kuduTable = kuduTable;
    this.client = client;
    this.enableInserts = enableInserts;
  }

  public CalciteKuduTableBuilder(KuduTable kuduTable, AsyncKuduClient client) {
    this(kuduTable, client, false);
  }

  public CalciteKuduTableBuilder setDescendingOrderedFieldIndices(List<Integer> descendingOrderedColumnIndices) {
    this.descendingOrderedFieldIndices = descendingOrderedColumnIndices;
    return this;
  }

  public CalciteKuduTableBuilder setCubeTables(List<CalciteKuduTable> cubeTables) {
    this.cubeTabes = cubeTables;
    return this;
  }

  public CalciteKuduTableBuilder setTableType(TableType tableType) {
    this.tableType = tableType;
    return this;
  }

  public CalciteKuduTableBuilder setTimestampColumnIndex(Integer timestampColumnIndex) {
    this.timestampColumnIndex = timestampColumnIndex;
    return this;
  }

  public CalciteKuduTableBuilder setEventTimeAggregationType(CubeTableInfo.EventTimeAggregationType eventTimeAggregationType) {
    this.eventTimeAggregationType = eventTimeAggregationType;
    return this;
  }

  public CalciteKuduTable build() {
    if (enableInserts) {
      return new CalciteModifiableKuduTable(kuduTable, client, descendingOrderedFieldIndices,
        timestampColumnIndex, cubeTabes, tableType, eventTimeAggregationType);
    }
    return new CalciteKuduTable(kuduTable, client, descendingOrderedFieldIndices,
      timestampColumnIndex, cubeTabes, tableType, eventTimeAggregationType);
  }
}
