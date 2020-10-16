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
package org.apache.calcite.jdbc;

import com.twilio.kudu.sql.CalciteModifiableKuduTable;
import com.twilio.kudu.sql.mutation.MutationState;

import java.util.HashMap;
import java.util.Map;

public class KuduMetaImpl extends CalciteMetaImpl {

  // used to keep track of state required to write rows to the kudu table
  // map from table name to mutation state
  private final Map<String, MutationState> mutationStateMap = new HashMap<>();

  public KuduMetaImpl(CalciteConnectionImpl connection) {
    super(connection);
  }

  @Override
  public void commit(ConnectionHandle ch) {
    for (MutationState mutationState : mutationStateMap.values()) {
      mutationState.flush();
    }
  }

  @Override
  public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
    if (connProps != null && connProps.isAutoCommit() != null && connProps.isAutoCommit()) {
      throw new UnsupportedOperationException("Autocommit is not supported");
    }
    return super.connectionSync(ch, connProps);
  }

  public MutationState getMutationState(CalciteModifiableKuduTable calciteKuduTable) {
    return mutationStateMap.computeIfAbsent(calciteKuduTable.getKuduTable().getName(),
        (k) -> new MutationState(calciteKuduTable));
  }

}
