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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaSpecificDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Factory that uses {@link CalciteJdbc41Factory} as a delegate so that we can
 * create our own {@link KuduCalciteConnectionImpl} that exposes
 * {@link KuduMetaImpl}
 */
public class KuduCalciteFactory extends CalciteFactory {

  private final CalciteJdbc41Factory delegate;

  public KuduCalciteFactory() {
    this(4, 1);
  }

  public KuduCalciteFactory(int major, int minor) {
    super(major, minor);
    delegate = new CalciteJdbc41Factory(major, minor);
  }

  @Override
  public AvaticaConnection newConnection(UnregisteredDriver driver, AvaticaFactory factory, String url, Properties info,
      CalciteSchema rootSchema, JavaTypeFactory typeFactory) {
    return new KuduCalciteConnectionImpl((Driver) driver, factory, url, info, rootSchema, typeFactory);
  }

  @Override
  public AvaticaStatement newStatement(AvaticaConnection connection, Meta.StatementHandle h, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) {
    return delegate.newStatement(connection, h, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection connection, Meta.StatementHandle h,
      Meta.Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return delegate.newPreparedStatement(connection, h, signature, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  @Override
  public AvaticaResultSet newResultSet(AvaticaStatement statement, QueryState state, Meta.Signature signature,
      TimeZone timeZone, Meta.Frame firstFrame) throws SQLException {
    return delegate.newResultSet(statement, state, signature, timeZone, firstFrame);
  }

  @Override
  public AvaticaSpecificDatabaseMetaData newDatabaseMetaData(AvaticaConnection connection) {
    return delegate.newDatabaseMetaData(connection);
  }

  @Override
  public ResultSetMetaData newResultSetMetaData(AvaticaStatement statement, Meta.Signature signature) {
    return delegate.newResultSetMetaData(statement, signature);
  }

}
