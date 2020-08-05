package org.apache.calcite.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta;

import java.util.Properties;

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

  @Override protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
      case JDBC_30:
      case JDBC_40:
        throw new IllegalArgumentException("JDBC version not supported: "
          + jdbcVersion);
      case JDBC_41:
      default:
        return KuduCalciteJdbc41Factory.class.getName();
    }
  }

}
