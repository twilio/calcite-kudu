package com.twilio.raas.sql;

import com.twilio.raas.dataloader.DataLoader;
import com.twilio.raas.dataloader.Scenario;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ScenarioIT {

  private static final String DATASET_NAME = "ReportCenter.OutboundMessages";
  public static final String ACCOUNT_SID = "AC3b1ebbfc4cd2fc2485ed634000000001";

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();

  @BeforeClass
  public static void setup() throws Exception {
    ColumnTypeAttributes decimalTypeAttribute =
      new ColumnTypeAttributes.ColumnTypeAttributesBuilder().scale(6).precision(22).build();

    // create fact table
    final List<ColumnSchema> columns = Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("account_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("date_created", Type.UNIXTIME_MICROS).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("mcc", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("mnc", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("sub_account_sid", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("npa_nxx", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("error_code", Type.INT32).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("status", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("msg_app_sid", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("sender", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("recipient", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("phone_number", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("channel", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("from_cc", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("to_cc", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("num_segments", Type.INT32).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("base_price", Type.DECIMAL).nullable(true)
        .typeAttributes(decimalTypeAttribute).build(),
      new ColumnSchema.ColumnSchemaBuilder("feedback_outcome", Type.STRING).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("attempt", Type.INT32).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("provide_feedback", Type.INT8).nullable(true).build()
    );

    testHarness.getClient().createTable(DATASET_NAME, new Schema(columns),
      new org.apache.kudu.client.CreateTableOptions()
        .addHashPartitions(Arrays.asList("account_sid"), 2)
        .setRangePartitionColumns(Arrays.asList("date_created"))
        .setNumReplicas(1));

    // create Feedback cube
    final List<ColumnSchema> feedbackColumns = Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("account_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("date_created", Type.UNIXTIME_MICROS).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("sub_account_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("msg_app_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("error_code", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("to_cc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("mcc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("mnc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("provide_feedback", Type.INT8).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("feedback_outcome", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("count_records", Type.INT64).nullable(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("sum_base_price", Type.DECIMAL).nullable(true)
        .typeAttributes(decimalTypeAttribute).build()
    );

    testHarness.getClient().createTable("OutboundMessages-Feedback-Aggregation", new Schema(feedbackColumns),
      new org.apache.kudu.client.CreateTableOptions()
        .addHashPartitions(Arrays.asList("account_sid"), 2)
        .setRangePartitionColumns(Arrays.asList("date_created"))
        .setNumReplicas(1));

    // create MessagingApp cube
    final List<ColumnSchema> messagingAppColumns = Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("account_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("date_created", Type.UNIXTIME_MICROS).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("sub_account_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("msg_app_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("status", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("error_code", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("to_cc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("channel", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("num_segments", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("mcc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("mnc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("count_records", Type.INT64).nullable(true).build()
    );

    testHarness.getClient().createTable("OutboundMessages-MessagingApp-Aggregation", new Schema(messagingAppColumns),
      new org.apache.kudu.client.CreateTableOptions()
        .addHashPartitions(Arrays.asList("account_sid"), 2)
        .setRangePartitionColumns(Arrays.asList("date_created"))
        .setNumReplicas(1));

    // create PhoneNumber cube
    final List<ColumnSchema> phoneNumberColumns = Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("account_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("date_created", Type.UNIXTIME_MICROS).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("sub_account_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("msg_app_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("phone_number", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("status", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("error_code", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("to_cc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("channel", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("num_segments", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("mcc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("mnc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("count_records", Type.INT64).nullable(true).build()
    );

    testHarness.getClient().createTable("OutboundMessages-PhoneNumber-Aggregation", new Schema(phoneNumberColumns),
      new org.apache.kudu.client.CreateTableOptions()
        .addHashPartitions(Arrays.asList("account_sid"), 2)
        .setRangePartitionColumns(Arrays.asList("date_created"))
        .setNumReplicas(1));

    // create SubAccount cube
    final List<ColumnSchema> subAccountColumns = Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("account_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("date_created", Type.UNIXTIME_MICROS).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("sub_account_sid", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("status", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("error_code", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("to_cc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("channel", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("num_segments", Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("mcc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("mnc", Type.STRING).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("count_records", Type.INT64).nullable(true).build()
    );

    testHarness.getClient().createTable("OutboundMessages-SubAccount-Aggregation", new Schema(subAccountColumns),
      new org.apache.kudu.client.CreateTableOptions()
        .addHashPartitions(Arrays.asList("account_sid"), 2)
        .setRangePartitionColumns(Arrays.asList("date_created"))
        .setNumReplicas(1));

    final Schema carrierNetworkSchema = new Schema(
      Arrays.asList(new ColumnSchema.ColumnSchemaBuilder("mcc", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("mnc", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("network", Type.STRING).build(),
        new ColumnSchema.ColumnSchemaBuilder("country", Type.STRING).build()));
    final org.apache.kudu.client.CreateTableOptions tableOptions = new org.apache.kudu.client.CreateTableOptions()
      .addHashPartitions(Arrays.asList("mcc", "mnc"), 2).setNumReplicas(1);
    testHarness.getClient()
      .createTable("CarrierLocation", carrierNetworkSchema, tableOptions);
  }

  @Test
  public void testScenario() throws IOException, SQLException {
    Scenario scenario = Scenario.loadScenario(this.getClass().getResource("/scenarios/ReportCenter" +
      ".OutboundMessages.json"));
    String url = String.format(JDBCUtil.CALCITE_MODEL_TEMPLATE_INSERT_ENABLED,
      testHarness.getMasterAddressesAsString());

    // load data
    new DataLoader(url, scenario).loadData();
    // verify data was written
    try (Connection conn = DriverManager.getConnection(url)) {
      ResultSet rs =
        conn.createStatement().executeQuery("SELECT COUNT(*) FROM \"" + DATASET_NAME + "\"");
      assertTrue(rs.next());
      assertEquals(scenario.getNumRows(), rs.getInt(1));
      assertFalse(rs.next());
    }
  }

}
