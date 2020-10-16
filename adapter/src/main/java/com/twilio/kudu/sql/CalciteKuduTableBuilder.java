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

import com.twilio.kudu.sql.metadata.CubeTableInfo;
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

  public CalciteKuduTableBuilder(KuduTable kuduTable, AsyncKuduClient client, boolean enableInserts) {
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

  public CalciteKuduTableBuilder setEventTimeAggregationType(
      CubeTableInfo.EventTimeAggregationType eventTimeAggregationType) {
    this.eventTimeAggregationType = eventTimeAggregationType;
    return this;
  }

  public CalciteKuduTable build() {
    if (enableInserts) {
      return new CalciteModifiableKuduTable(kuduTable, client, descendingOrderedFieldIndices, timestampColumnIndex,
          cubeTabes, tableType, eventTimeAggregationType);
    }
    return new CalciteKuduTable(kuduTable, client, descendingOrderedFieldIndices, timestampColumnIndex, cubeTabes,
        tableType, eventTimeAggregationType);
  }
}
