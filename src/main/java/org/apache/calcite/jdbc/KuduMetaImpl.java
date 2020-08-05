package org.apache.calcite.jdbc;

import com.twilio.raas.sql.CalciteModifiableKuduTable;
import com.twilio.raas.sql.mutation.MutationState;

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
