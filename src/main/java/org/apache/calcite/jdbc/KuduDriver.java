package org.apache.calcite.jdbc;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta;

/**
 * Customized driver so that we can use our own meta implementation
 */
public class KuduDriver extends Driver {

  public static final String CONNECT_STRING_PREFIX = "jdbc:kudu:";

  static {
    new KuduDriver().register();
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  public Meta createMeta(AvaticaConnection connection) {
    return new KuduMetaImpl((CalciteConnectionImpl) connection);
  }
}
