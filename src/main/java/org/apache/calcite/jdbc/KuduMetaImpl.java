package org.apache.calcite.jdbc;

import com.twilio.raas.sql.CalciteModifiableKuduTable;
import org.apache.calcite.linq4j.Enumerable;

import java.lang.reflect.Field;

public class KuduMetaImpl extends CalciteMetaImpl {

  public KuduMetaImpl(CalciteConnectionImpl connection) {
    super(connection);
  }

  @Override
  public void commit(ConnectionHandle ch) {
    // TODO figure out if there is a way to use only the table(s) that were changed in the
    //  current connection
    Enumerable<MetaTable> metaTables = tables("KUDU");
    for (MetaTable metaTable : metaTables) {
      try {
        final Class<?> calciteMetaTableClass = Class.forName(CalciteMetaImpl.class.getName()+
          "$CalciteMetaTable");
        Field calciteTableField = calciteMetaTableClass.getDeclaredField("calciteTable");
        calciteTableField.setAccessible(true);
        Object table = calciteTableField.get(calciteMetaTableClass.cast(metaTable));
        if (table instanceof  CalciteModifiableKuduTable) {
          CalciteModifiableKuduTable calciteKuduTable = (CalciteModifiableKuduTable) table;
          // flush all pending mutations
          calciteKuduTable.getMutationState().flush();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public ConnectionProperties connectionSync(ConnectionHandle ch, ConnectionProperties connProps) {
    if (connProps != null && connProps.isAutoCommit() != null && connProps.isAutoCommit()) {
      throw new UnsupportedOperationException("Autocommit is not supported");
    }
    return super.connectionSync(ch, connProps);
  }

}
