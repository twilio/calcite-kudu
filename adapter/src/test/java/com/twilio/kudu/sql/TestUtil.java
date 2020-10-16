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
package com.twilio.kudu.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TestUtil {

  public static void printResultSet(ResultSet rs) throws SQLException {
    ResultSetMetaData rsmd = rs.getMetaData();
    int columnCount = rsmd.getColumnCount();
    boolean printedColNames = false;
    while (rs.next()) {
      if (!printedColNames) {
        for (int i = 1; i <= columnCount; ++i) {
          if (i > 1) {
            System.out.print(", ");
          }
          System.out.print(rsmd.getColumnName(i));
        }
        System.out.println();
        printedColNames = true;
      }
      for (int i = 1; i <= columnCount; ++i) {
        if (i > 1) {
          System.out.print(", ");
        }
        String columnValue = rs.getString(i);
        System.out.print(columnValue);
      }
      System.out.println();
    }
  }

}
