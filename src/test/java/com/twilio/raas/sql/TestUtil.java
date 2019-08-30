package com.twilio.raas.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class TestUtil {

    public static String getExplainPlan(ResultSet rs) throws SQLException {
        StringBuilder buf = new StringBuilder();
        while (rs.next()) {
            buf.append(rs.getString(1));
            buf.append('\n');
        }
        if (buf.length() > 0) {
            buf.setLength(buf.length()-1);
        }
        return buf.toString();
    }

    public static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        boolean printedColNames = false;
        while (rs.next()) {
            if (!printedColNames) {
                for (int i = 1; i < columnCount; ++i) {
                    if (i > 1) {
                        System.out.print(", ");
                    }
                    System.out.print(rsmd.getColumnName(i));
                }
                System.out.println();
                printedColNames = true;
            }
            for (int i = 1; i < columnCount; ++i) {
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
