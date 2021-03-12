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

import com.twilio.kudu.dataloader.DataLoader;
import com.twilio.kudu.dataloader.Scenario;
import com.twilio.kudu.sql.schema.DefaultKuduSchemaFactory;
import com.twilio.kudu.sql.schema.KuduSchema;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ScenarioIT {

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();
  private static String JDBC_URL;

  @BeforeClass
  public static void setup() {
    String urlFormat = JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED + ";schema."
        + KuduSchema.CREATE_DUMMY_PARTITION_FLAG + "=false";
    JDBC_URL = String.format(urlFormat, DefaultKuduSchemaFactory.class.getName(),
        testHarness.getMasterAddressesAsString());
  }

  @Test
  public void testOutboundMessages() throws IOException, SQLException {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String ddl = "CREATE TABLE \"ReportCenter.OutboundMessages\" (" + "\"account_sid\" VARCHAR, "
          + "\"date_created\" TIMESTAMP DESC ROW_TIMESTAMP, " + "\"sid\" VARCHAR, " + "\"mcc\" VARCHAR, "
          + "\"mnc\" VARCHAR, " + "\"sub_account_sid\" VARCHAR, " + "\"npa_nxx\" VARCHAR, " + "\"error_code\" INTEGER, "
          + "\"status\" VARCHAR, " + "\"msg_app_sid\" VARCHAR, " + "\"sender\" VARCHAR, " + "\"recipient\" VARCHAR, "
          + "\"phone_number\" VARCHAR, " + "\"channel\" VARCHAR, " + "\"from_cc\" VARCHAR, " + "\"to_cc\" VARCHAR, "
          + "\"num_segments\" INTEGER, " + "\"base_price\" DECIMAL(22, 6), " + "\"feedback_outcome\" VARCHAR, "
          + "\"attempt\" INTEGER, " + "\"provide_feedback\" TINYINT, "
          + "PRIMARY KEY (\"account_sid\", \"date_created\", \"sid\"))"
          + "PARTITION BY HASH (\"account_sid\") PARTITIONS 2 NUM_REPLICAS 1";
      conn.createStatement().execute(ddl);

      String ddl1 = "CREATE MATERIALIZED VIEW \"MessagingAppHourly\" AS " + "SELECT COUNT(*) as \"count_records\" "
          + "FROM \"ReportCenter.OutboundMessages\" "
          + "GROUP BY \"account_sid\", FLOOR(\"date_created\" TO HOUR), \"sub_account_sid\", "
          + "\"msg_app_sid\", \"status\", \"error_code\", \"to_cc\", \"channel\", \"num_segments\", \"mcc\", \"mnc\"";
      conn.createStatement().execute(ddl1);

      String ddl2 = "CREATE MATERIALIZED VIEW \"Feedback\" AS "
          + "SELECT SUM(\"base_price\") as \"sum_base_price\", COUNT(*) as \"count_records\" "
          + "FROM \"ReportCenter.OutboundMessages\" "
          + "GROUP BY \"account_sid\", FLOOR(\"date_created\" TO DAY), \"sub_account_sid\", "
          + "\"msg_app_sid\", \"error_code\", \"to_cc\", \"mcc\", \"mnc\", \"provide_feedback\", \"feedback_outcome\"";
      conn.createStatement().execute(ddl2);

      String ddl3 = "CREATE MATERIALIZED VIEW \"MessagingApp\" AS " + "SELECT COUNT(*) as \"count_records\" "
          + "FROM \"ReportCenter.OutboundMessages\" "
          + "GROUP BY \"account_sid\", FLOOR(\"date_created\" TO DAY), \"sub_account_sid\", "
          + "\"msg_app_sid\", \"status\", \"error_code\", \"to_cc\", \"channel\", \"num_segments\", \"mcc\", \"mnc\"";
      conn.createStatement().execute(ddl3);

      String ddl4 = "CREATE MATERIALIZED VIEW \"PhoneNumber\" AS " + "SELECT COUNT(*) as \"count_records\" "
          + "FROM \"ReportCenter.OutboundMessages\" "
          + "GROUP BY \"account_sid\", FLOOR(\"date_created\" TO DAY), \"sub_account_sid\", "
          + "\"msg_app_sid\", \"phone_number\", \"status\", \"error_code\", \"to_cc\", \"channel\", "
          + "\"num_segments\", \"mcc\", \"mnc\"";
      conn.createStatement().execute(ddl4);

      String ddl5 = "CREATE MATERIALIZED VIEW \"SubAccount\" AS " + "SELECT COUNT(*) as \"count_records\" "
          + "FROM \"ReportCenter.OutboundMessages\" "
          + "GROUP BY \"account_sid\", FLOOR(\"date_created\" TO DAY), \"sub_account_sid\", "
          + "\"status\", \"error_code\", \"to_cc\", \"channel\", \"num_segments\", \"mcc\", \"mnc\"";
      conn.createStatement().execute(ddl5);
    }

    Scenario scenario = Scenario
        .loadScenario(this.getClass().getResource("/scenarios/ReportCenter" + ".OutboundMessages.json"));

    // load data
    new DataLoader(JDBC_URL, scenario).loadData(Optional.empty());
    // verify data was written
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM \"ReportCenter.OutboundMessages\"");
      assertTrue(rs.next());
      assertEquals(scenario.getNumRows(), rs.getInt(1));
      assertFalse(rs.next());

      // verify the cube row counts match
      rs = conn.createStatement().executeQuery(
          "SELECT SUM(count_records) FROM " + "\"ReportCenter.OutboundMessages-MessagingAppHourly-Hour-Aggregation\"");
      assertTrue(rs.next());
      assertEquals(scenario.getNumRows(), rs.getInt(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT SUM(count_records) FROM " + "\"ReportCenter.OutboundMessages-Feedback-Day-Aggregation\"");
      assertTrue(rs.next());
      assertEquals(scenario.getNumRows(), rs.getInt(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT SUM(count_records) FROM " + "\"ReportCenter.OutboundMessages-MessagingApp-Day-Aggregation\"");
      assertTrue(rs.next());
      assertEquals(scenario.getNumRows(), rs.getInt(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT SUM(count_records) FROM " + "\"ReportCenter.OutboundMessages-PhoneNumber-Day-Aggregation\"");
      assertTrue(rs.next());
      assertEquals(scenario.getNumRows(), rs.getInt(1));
      assertFalse(rs.next());

      rs = conn.createStatement().executeQuery(
          "SELECT SUM(count_records) FROM " + "\"ReportCenter.OutboundMessages-SubAccount-Day-Aggregation\"");
      assertTrue(rs.next());
      assertEquals(scenario.getNumRows(), rs.getInt(1));
      assertFalse(rs.next());
    }
  }

  @Test
  public void testUsageReportTransactions() throws IOException, SQLException {
    Scenario scenario = Scenario
        .loadScenario(this.getClass().getResource("/scenarios/ReportCenter" + ".UsageReportTransactions.json"));

    // verify data was written
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String ddl = "CREATE TABLE \"ReportCenter.UsageReportTransactions\" (" + "\"usage_account_sid\" VARCHAR, "
          + "\"date_initiated\" TIMESTAMP DESC ROW_TIMESTAMP, " + "\"transaction_id\" VARCHAR, "
          + "\"units\" SMALLINT, " + "\"billable_item\" VARCHAR, " + "\"calculated_sid\" VARCHAR, "
          + "\"sub_account_sid\" VARCHAR, " + "\"phonenumber\" VARCHAR, " + "\"to\" VARCHAR, " + "\"from\" VARCHAR, "
          + "\"amount\" DECIMAL(22, 6), " + "\"quantity\" DECIMAL(22, 6), "
          + "PRIMARY KEY (\"usage_account_sid\", \"date_initiated\", \"transaction_id\"))"
          + "PARTITION BY HASH (\"usage_account_sid\") PARTITIONS 2 NUM_REPLICAS 1";
      conn.createStatement().execute(ddl);

      String ddl2 = "CREATE MATERIALIZED VIEW \"Cube\" AS SELECT "
          + "SUM(\"amount\") as \"sum_amount\", SUM(\"quantity\") as \"sum_quantity\", COUNT(*) as \"count_records\""
          + "FROM \"ReportCenter.UsageReportTransactions\" "
          + "GROUP BY \"usage_account_sid\", FLOOR(\"date_initiated\" TO DAY), \"billable_item\", \"units\", \"sub_account_sid\"";
      conn.createStatement().execute(ddl2);
    }

    // load data
    new DataLoader(JDBC_URL, scenario).loadData(Optional.empty());

    // verify data was written
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement()
          .executeQuery("SELECT COUNT(*) FROM \"ReportCenter.UsageReportTransactions\"");
      assertTrue(rs.next());
      assertEquals(scenario.getNumRows(), rs.getInt(1));
      assertFalse(rs.next());

      // verify the cube row counts match
      rs = conn.createStatement().executeQuery(
          "SELECT SUM(\"count_records\") FROM " + "\"ReportCenter.UsageReportTransactions-Cube-Day-Aggregation\"");
      assertTrue(rs.next());
      assertEquals(scenario.getNumRows(), rs.getInt(1));
      assertFalse(rs.next());
    }
  }

}
