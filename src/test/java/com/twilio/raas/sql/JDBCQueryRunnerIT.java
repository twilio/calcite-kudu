package com.twilio.raas.sql;

import org.apache.kudu.client.Upsert;
import org.apache.kudu.Schema;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.test.KuduTestHarness;
import org.slf4j.Logger;
import java.sql.Timestamp;
import org.junit.runners.JUnit4;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduSession;
import java.util.List;
import org.slf4j.LoggerFactory;
import java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.AfterClass;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import java.sql.SQLException;

@RunWith(JUnit4.class)
public final class JDBCQueryRunnerIT {
  private static final Logger logger = LoggerFactory.getLogger(JDBCQueryRunnerIT.class);

  public static String FIRST_SID = "SM1234857";
  public static String SECOND_SID = "SM123485789";

  public static String ACCOUNT_SID = "AC1234567";

  @ClassRule
  public static KuduTestHarness testHarness = new KuduTestHarness();
  public static final String BASE_TABLE_NAME = "ReportCenter.DeliveredMessages";

  public static KuduTable TABLE;

  @BeforeClass
  public static void setup() throws Exception {
    final List<ColumnSchema> columns = Arrays.asList(
						     new ColumnSchema.ColumnSchemaBuilder("account_sid", Type.STRING).key(true).build(),
						     new ColumnSchema.ColumnSchemaBuilder("date_created", Type.UNIXTIME_MICROS).key(true).build(),
						     new ColumnSchema.ColumnSchemaBuilder("sid", Type.STRING).key(true).build());
    
    testHarness.getClient().createTable(BASE_TABLE_NAME, new Schema(columns),
					new org.apache.kudu.client.CreateTableOptions()
					.addHashPartitions(Arrays.asList("account_sid"), 5)
					
					.setNumReplicas(1));
    final AsyncKuduSession insertSession = testHarness.getAsyncClient().newSession();
    TABLE = testHarness.getClient().openTable(BASE_TABLE_NAME);

    final Upsert firstRowOp = TABLE.newUpsert();
    final PartialRow firstRowWrite = firstRowOp.getRow();
    firstRowWrite.addString("account_sid", JDBCQueryRunnerIT.ACCOUNT_SID);
    firstRowWrite.addString("sid", JDBCQueryRunnerIT.FIRST_SID);
    firstRowWrite.addTimestamp("date_created", new Timestamp(System.currentTimeMillis()));
    insertSession.apply(firstRowOp).join();

    final Upsert secondRowOp = TABLE.newUpsert();
    final PartialRow secondRowWrite = secondRowOp.getRow();
    secondRowWrite.addString("account_sid", JDBCQueryRunnerIT.ACCOUNT_SID);
    secondRowWrite.addString("sid", "SM123485789");
    secondRowWrite.addTimestamp("date_created", new Timestamp(System.currentTimeMillis()));
    insertSession.apply(secondRowOp).join();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testHarness.getClient().deleteTable(BASE_TABLE_NAME);
  }

  @Test
  public void testQuery() throws Exception {
    try (final JDBCQueryRunner runner = new JDBCQueryRunner(testHarness.getMasterAddressesAsString(), 1)) {
      List<Map<String, Object>> records = runner
	.executeSql("SELECT account_sid, date_created, sid FROM kudu.\"ReportCenter.DeliveredMessages\"", (rs -> {
	      final Map<String, Object> record = new HashMap<>();
	      try {
		record.put("account_sid", rs.getString(1));
		record.put("date_created", rs.getLong(2));
		record.put("sid", rs.getString(3));
	      }
	      catch (SQLException failed) {
		// swallow it.
	      }
	      return record;
	    }))
	  .toCompletableFuture().get();
	Assert.assertEquals("should get the rows back",
			    2, records.size());
	Assert.assertEquals("First record's account sid should match",
			    ACCOUNT_SID, records.get(0).get("account_sid"));
	Assert.assertEquals("Second record's account sid should match",
			    ACCOUNT_SID, records.get(1).get("account_sid"));
    }
  }
}
