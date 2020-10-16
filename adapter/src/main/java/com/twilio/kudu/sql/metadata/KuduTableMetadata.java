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

import java.util.ArrayList;
import java.util.List;

public class KuduTableMetadata {

  private final List<CubeTableInfo> cubeTableInfoList;
  private final List<String> descendingOrderedColumnNames;
  private final String timestampColumnName;

  private KuduTableMetadata(List<CubeTableInfo> cubeTableInfoList, List<String> descendingOrderedColumnNames,
      String timestampColumnName) {
    this.descendingOrderedColumnNames = descendingOrderedColumnNames;
    this.cubeTableInfoList = cubeTableInfoList;
    this.timestampColumnName = timestampColumnName;
  }

  public List<String> getDescendingOrderedColumnNames() {
    return descendingOrderedColumnNames;
  }

  public List<CubeTableInfo> getCubeTableInfo() {
    return cubeTableInfoList;
  }

  public String getTimestampColumnName() {
    return timestampColumnName;
  }

  public static class KuduTableMetadataBuilder {
    private List<CubeTableInfo> cubeTableInfoList = new ArrayList<>();
    private List<String> descendingOrderedColumnNames = new ArrayList<>();
    private String timestampColumnName = null;

    public KuduTableMetadataBuilder setCubeTableInfoList(List<CubeTableInfo> cubeTableInfoList) {
      this.cubeTableInfoList = cubeTableInfoList;
      return this;
    }

    public KuduTableMetadataBuilder setDescendingOrderedColumnNames(List<String> descendingOrderedColumnNames) {
      this.descendingOrderedColumnNames = descendingOrderedColumnNames;
      return this;
    }

    public KuduTableMetadataBuilder setTimestampColumnName(String timestampColumnName) {
      this.timestampColumnName = timestampColumnName;
      return this;
    }

    public KuduTableMetadata build() {
      return new KuduTableMetadata(cubeTableInfoList, descendingOrderedColumnNames, timestampColumnName);
    }
  }
}