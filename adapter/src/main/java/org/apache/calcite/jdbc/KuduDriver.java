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

import org.apache.calcite.prepare.KuduPrepareImpl;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.linq4j.function.Function0;

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

  @Override
  protected String getFactoryClassName(JdbcVersion jdbcVersion) {
    switch (jdbcVersion) {
    case JDBC_30:
    case JDBC_40:
      throw new IllegalArgumentException("JDBC version not supported: " + jdbcVersion);
    case JDBC_41:
    default:
      return KuduCalciteFactory.class.getName();
    }
  }

  @Override
  protected Function0<CalcitePrepare> createPrepareFactory() {
    return (Function0<CalcitePrepare>) () -> new KuduPrepareImpl();
  }

}
