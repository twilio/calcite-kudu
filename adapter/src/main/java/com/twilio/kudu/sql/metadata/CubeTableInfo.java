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
package com.twilio.kudu.sql.metadata;

import java.util.List;

public class CubeTableInfo {

  public String tableName;
  public EventTimeAggregationType eventTimeAggregationType;
  public List<String> descendingOrderedColumnNames;
  public String timestampColumnName;

  public CubeTableInfo(String tableName, EventTimeAggregationType eventTimeAggregationType,
      final List<String> descendingOrderedColumnNames, final String timestampColumnName) {
    this.eventTimeAggregationType = eventTimeAggregationType;
    this.tableName = tableName;
    this.descendingOrderedColumnNames = descendingOrderedColumnNames;
    this.timestampColumnName = timestampColumnName;
  }

  public enum EventTimeAggregationType {
    year, month, day, hour, minute, second;
  }
}