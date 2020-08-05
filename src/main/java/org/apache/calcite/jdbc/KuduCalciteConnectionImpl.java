package org.apache.calcite.jdbc;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaFactory;

import java.util.Properties;

public class KuduCalciteConnectionImpl extends CalciteConnectionImpl {

  protected KuduCalciteConnectionImpl(Driver driver, AvaticaFactory factory, String url,
                                      Properties info, CalciteSchema rootSchema,
                                      JavaTypeFactory typeFactory) {
    super(driver, factory, url, info, rootSchema, typeFactory);
  }

  public KuduMetaImpl getMeta() {
    return (KuduMetaImpl) meta();
  }

}
