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
import org.apache.calcite.prepare.KuduPrepareImpl;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.ImmutableBeans;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ScenarioIT {

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();
  private static String JDBC_URL;
  private static Scenario usageReportTransactionScenario;

  @BeforeClass
  public static void setup() throws SQLException, IOException {
    initializeHints();
    String urlFormat = JDBCUtil.CALCITE_MODEL_TEMPLATE_DML_DDL_ENABLED + ";schema."
        + KuduSchema.CREATE_DUMMY_PARTITION_FLAG + "=false";
    JDBC_URL = String.format(urlFormat, DefaultKuduSchemaFactory.class.getName(),
        testHarness.getMasterAddressesAsString());

    // create the UsageReportTransactions table
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

      usageReportTransactionScenario = Scenario
          .loadScenario(ScenarioIT.class.getResource("/scenarios/ReportCenter.UsageReportTransactions.json"));
      // load data
      new DataLoader(JDBC_URL, usageReportTransactionScenario).loadData(Optional.empty());
    }
  }

  private static void setFinalStatic(Field field, Object newValue) throws Exception {
    field.setAccessible(true);

    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

    field.set(null, newValue);
  }

  // TODO figure out a better way to set the HintStrategyTable for sql queries
  public static void initializeHints() {
    try {
      SqlToRelConverter.Config CONFIG_MODIFIED = ImmutableBeans.create(SqlToRelConverter.Config.class)
          .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
          .withRelBuilderConfigTransform(c -> c.withPushJoinCondition(true))
          .withHintStrategyTable(KuduQuery.KUDU_HINT_STRATEGY_TABLE);
      // change SqlToRelConverter.CONFIG to use one that has the above
      // HintStrategyTable
      setFinalStatic(SqlToRelConverter.class.getDeclaredField("CONFIG"), CONFIG_MODIFIED);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
  public void testKuduNestedLoopJoin() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      String ddl2 = "CREATE TABLE \"OrganizationAccounts\" (" + "\"organization_sid\" VARCHAR, "
          + "\"account_sid\" VARCHAR, " + "PRIMARY KEY (\"organization_sid\", \"account_sid\"))"
          + "PARTITION BY HASH (\"organization_sid\") PARTITIONS 2 NUM_REPLICAS 1";
      conn.createStatement().execute(ddl2);
      // create an organization with the two accounts used to load data
      PreparedStatement stmt = conn.prepareStatement("INSERT INTO \"OrganizationAccounts\" " + "VALUES (?,?)");
      stmt.setString(1, "ORGANIZATION_1");
      stmt.setString(2, "ACCOUNT_1");
      stmt.execute();
      stmt.setString(1, "ORGANIZATION_1");
      stmt.setString(2, "ACCOUNT_2");
      stmt.execute();
      conn.commit();

      String sqlFormat = "SELECT %s"
          + "USAGE_ACCOUNT_SID, SUM(\"quantity\") AS \"SUM_QUANTITY\" FROM \"OrganizationAccounts\" "
          + "JOIN \"ReportCenter.UsageReportTransactions\"  ON \"USAGE_ACCOUNT_SID\"  =  \"OrganizationAccounts\".ACCOUNT_SID"
          + " WHERE  ORGANIZATION_SID='ORGANIZATION_1'" + " AND \"DATE_INITIATED\" >= TIMESTAMP '2020-06-01 00:00:00'"
          + " AND \"DATE_INITIATED\" < TIMESTAMP '2020-06-15 00:00:00'" + " GROUP BY USAGE_ACCOUNT_SID";

      // force the plan to use KuduNestedJoin
      String hint = "/*+ USE_KUDU_NESTED_JOIN */";
      String expectedPlan = "EnumerableAggregate(group=[{0}], SUM_QUANTITY=[SUM($1)])\n"
          + "  EnumerableCalc(expr#0..4=[{inputs}], USAGE_ACCOUNT_SID=[$t2], QUANTITY=[$t4])\n"
          + "    KuduNestedJoin(condition=[=($2, $1)], joinType=[inner], batchSize=[100])\n"
          + "      KuduToEnumerableRel\n"
          + "        KuduFilterRel(ScanToken 1=[organization_sid EQUAL ORGANIZATION_1])\n"
          + "          KuduQuery(table=[[kudu, OrganizationAccounts]])\n" + "      KuduToEnumerableRel\n"
          + "        KuduProjectRel(USAGE_ACCOUNT_SID=[$0], DATE_INITIATED=[$1], QUANTITY=[$11])\n"
          + "          KuduFilterRel(ScanToken 1=[date_initiated GREATER_EQUAL 1590969600000000, date_initiated LESS 1592179200000000])\n"
          + "            KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";
      String sql = String.format(sqlFormat, hint);
      ResultSet rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      String plan = SqlUtil.getExplainPlan(rs);
      assertEquals("Plan does not match", expectedPlan, plan);
      rs = conn.createStatement().executeQuery(sql);
      List<List<Object>> kuduNestedJoinResult = SqlUtil.getResult(rs);

      // running the query without the hint should use the regular EnumerableHashJoin
      sql = String.format(sqlFormat, "");
      expectedPlan = "EnumerableAggregate(group=[{0}], SUM_QUANTITY=[SUM($1)])\n"
          + "  EnumerableCalc(expr#0..4=[{inputs}], USAGE_ACCOUNT_SID=[$t2], QUANTITY=[$t4])\n"
          + "    EnumerableHashJoin(condition=[=($1, $2)], joinType=[inner])\n" + "      KuduToEnumerableRel\n"
          + "        KuduFilterRel(ScanToken 1=[organization_sid EQUAL ORGANIZATION_1])\n"
          + "          KuduQuery(table=[[kudu, OrganizationAccounts]])\n" + "      KuduToEnumerableRel\n"
          + "        KuduProjectRel(USAGE_ACCOUNT_SID=[$0], DATE_INITIATED=[$1], QUANTITY=[$11])\n"
          + "          KuduFilterRel(ScanToken 1=[date_initiated GREATER_EQUAL 1590969600000000, date_initiated LESS 1592179200000000])\n"
          + "            KuduQuery(table=[[kudu, ReportCenter.UsageReportTransactions]])\n";
      rs = conn.createStatement().executeQuery("EXPLAIN PLAN FOR " + sql);
      plan = SqlUtil.getExplainPlan(rs);
      assertEquals("Plan does not match", expectedPlan, plan);
      rs = conn.createStatement().executeQuery(sql);
      List<List<Object>> hashJoinResult = SqlUtil.getResult(rs);

      assertEquals("Results do not match", hashJoinResult, kuduNestedJoinResult);
    }
  }

  @Test
  public void testUsageReportTransactions() throws IOException, SQLException {
    // verify data was written
    try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
      ResultSet rs = conn.createStatement()
          .executeQuery("SELECT COUNT(*) FROM \"ReportCenter.UsageReportTransactions\"");
      assertTrue(rs.next());
      assertEquals(usageReportTransactionScenario.getNumRows(), rs.getInt(1));
      assertFalse(rs.next());

      // verify the cube row counts match
      rs = conn.createStatement().executeQuery(
          "SELECT SUM(\"count_records\") FROM " + "\"ReportCenter.UsageReportTransactions-Cube-Day-Aggregation\"");
      assertTrue(rs.next());
      assertEquals(usageReportTransactionScenario.getNumRows(), rs.getInt(1));
      assertFalse(rs.next());
    }
  }

}
