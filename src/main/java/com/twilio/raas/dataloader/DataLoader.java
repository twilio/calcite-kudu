package com.twilio.raas.dataloader;

import com.twilio.raas.dataloader.generator.ColumnValueGenerator;
import com.twilio.raas.dataloader.generator.MultipleColumnValueGenerator;
import com.twilio.raas.dataloader.generator.UniformLongValueGenerator;
import com.twilio.raas.sql.CalciteModifiableKuduTable;
import com.twilio.raas.sql.schema.KuduSchemaFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import com.twilio.raas.sql.CalciteKuduTable;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.kudu.ColumnSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class DataLoader {

  private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);

  private final ExecutorService threadPool;
  private final CompletionService<Void> completionService;
  private final int threadPoolSize;

  private final Scenario scenario;
  private final CalciteKuduTable calciteKuduTable;
  private String url;
  private final int batchSize = 1000;
  private final long scenarioStartTimestamp;
  private final long scenarioEndTimestamp;
  private final long cubeGranularityFloorMod;

  public DataLoader(final String url, final Scenario scenario) throws SQLException {
    this(url, scenario, null);
  }

  public DataLoader(final String url, final Scenario scenario, Integer threadPoolSize) throws SQLException {
    // load all the CalciteKuduTables
    DriverManager.getConnection(url);
    this.scenario = scenario;
    this.calciteKuduTable =
      KuduSchemaFactory.INSTANCE.getTable(scenario.getTableName())
        .orElseThrow(() -> new RuntimeException("Table not found " + scenario.getTableName()));
    this.url = url;

    // we assume the second column is the timestamp column
    String timestampColumnName =
      calciteKuduTable.getKuduTable().getSchema().getColumnByIndex(1).getName();
    UniformLongValueGenerator timestampGenerator =
      (UniformLongValueGenerator) scenario.getColumnNameToValueGenerator().get(timestampColumnName);
    // initialize minValue and maxValue if the generator is a TimestampGenerator
    timestampGenerator.getColumnValue();
    this.scenarioStartTimestamp = timestampGenerator.minValue;
    this.scenarioEndTimestamp = timestampGenerator.maxValue;
    // pick the first cube to determine the FLOOR mod value
    // TODO figure out how to handle a mix of different cube time rollup granularities
    this.cubeGranularityFloorMod = calciteKuduTable.getCubeTables().isEmpty() ?
      DateTimeUtils.MILLIS_PER_SECOND :
      ((CalciteModifiableKuduTable) calciteKuduTable.getCubeTables().get(0)).getCubeMaintainer().getFloorMod();
    int numTasks =
      (int) Math.ceil((float) (scenarioEndTimestamp - scenarioStartTimestamp) / cubeGranularityFloorMod);

    // set thread pool to MIN(2 * number of cpus, numTasks) by default
    this.threadPoolSize = threadPoolSize!=null ? threadPoolSize :
      Math.min(Runtime.getRuntime().availableProcessors() * 2, numTasks);
    logger.info("Thread pool size {}", this.threadPoolSize);
    this.threadPool = Executors.newFixedThreadPool(this.threadPoolSize, new ThreadFactory() {
      int threadCounter = 0;
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, String.format("DataLoader-%s-%d",
          scenario.getTableName(), threadCounter++));
      }
    });
    this.completionService = new ExecutorCompletionService<>(threadPool);
  }

  private String buildSql() {
    StringBuilder builder = new StringBuilder();
    builder.append("UPSERT INTO \"");
    builder.append(calciteKuduTable.getKuduTable().getName());
    builder.append("\" (");
    boolean isFirst = true;
    for (ColumnSchema columnSchema : calciteKuduTable.getKuduTable().getSchema().getColumns()) {
      if (isFirst) {
        isFirst = false;
      }
      else {
        builder.append(",");
      }
      builder.append("\"").append(columnSchema.getName()).append("\"");
    }
    builder.append(") VALUES (");
    for (int i = 0; i < calciteKuduTable.getKuduTable().getSchema().getColumnCount(); i++) {
      if (i < calciteKuduTable.getKuduTable().getSchema().getColumnCount() - 1) {
        builder.append("?,");
      } else {
        builder.append("?)");
      }
    }
    return builder.toString();
  }

  private ColumnValueGenerator getColumnValueGenerator(String columnName) {
    if (!scenario.getColumnNameToValueGenerator().containsKey(columnName)) {
      for (MultipleColumnValueGenerator generator : scenario.getMultipleColumnValueGenerators()) {
        if (generator.getColumnNames().contains(columnName)) {
          return generator;
        }
      }
      throw new IllegalStateException("No generator found for column " + columnName);
    }
    return scenario.getColumnNameToValueGenerator().get(columnName);
  }

  private void bindValues(PreparedStatement statement,
                          UniformLongValueGenerator timestampGenerator) throws SQLException {
    if (scenario.getMultipleColumnValueGenerators()!=null) {
      for (MultipleColumnValueGenerator generator : scenario.getMultipleColumnValueGenerators()) {
        generator.reset();
      }
    }
    int count = 1;
    for (ColumnSchema columnSchema : calciteKuduTable.getKuduTable().getSchema().getColumns()) {
      final Object value;
      String columnName = columnSchema.getName();
      switch (columnSchema.getType()) {
        case INT8:
          Byte byteValue;
          value = getColumnValueGenerator(columnName).getColumnValue(columnName);
          if (value instanceof Byte) {
            byteValue = (Byte) value;
          }
          else {
            byteValue = ((Integer) value).byteValue();
          }
          if (byteValue == null) {
            statement.setNull(count, Types.TINYINT);
          } else {
            statement.setByte(count, byteValue);
          }
          break;
        case INT16:
          Short shortValue;
          value = getColumnValueGenerator(columnName).getColumnValue(columnName);
          if (value instanceof Short) {
            shortValue = (Short) value;
          }
          else {
            shortValue = ((Integer) value).shortValue();
          }
          if (shortValue == null) {
            statement.setNull(count, Types.SMALLINT);
          } else {
            statement.setShort(count, shortValue);
          }
          break;
        case INT32:
          Integer intValue = (Integer) getColumnValueGenerator(columnName).getColumnValue(columnName);
          if (intValue == null) {
            statement.setNull(count, Types.INTEGER);
          } else {
            statement.setInt(count, intValue);
          }
          break;
        case UNIXTIME_MICROS:
          // If we are using multiple threads have each thread write data for a non overlapping
          // time range. We assume the second column is the date partitioned columnLong longValue =
          Long timestampValue;
          if (count == 2 && threadPoolSize >1) {
            timestampValue = timestampGenerator.getColumnValue();
          }
          else {
            timestampValue = (Long) getColumnValueGenerator(columnName).getColumnValue(columnName);
          }
          if (timestampValue == null) {
            statement.setNull(count, Types.BIGINT);
          } else {
            statement.setLong(count, timestampValue);
          }
          break;
        case INT64:
          Long longValue = (Long) getColumnValueGenerator(columnName).getColumnValue(columnName);
          if (longValue == null) {
            statement.setNull(count, Types.BIGINT);
          } else {
            statement.setLong(count, longValue);
          }
          break;
        case STRING:
          String stringValue = (String) getColumnValueGenerator(columnName).getColumnValue(columnName);
          if (stringValue == null) {
            statement.setNull(count, Types.VARCHAR);
          } else {
            statement.setString(count, stringValue);
          }
          break;
        case BOOL:
          Boolean booleanValue = (Boolean) getColumnValueGenerator(columnName).getColumnValue(columnName);
          if (booleanValue == null) {
            statement.setNull(count, Types.VARCHAR);
          } else {
            statement.setBoolean(count, booleanValue);
          }
          break;
        case FLOAT:
          Float floatValue = (Float) getColumnValueGenerator(columnName).getColumnValue(columnName);
          if (floatValue == null) {
            statement.setNull(count, Types.FLOAT);
          } else {
            statement.setFloat(count, floatValue);
          }
          break;
        case DOUBLE:
          Double doubleVal = (Double) getColumnValueGenerator(columnName).getColumnValue(columnName);
          if (doubleVal == null) {
            statement.setNull(count, Types.DOUBLE);
          } else {
            statement.setDouble(count, doubleVal);
          }
          break;
        case DECIMAL:
          BigDecimal decimalVal = (BigDecimal) getColumnValueGenerator(columnName).getColumnValue(columnName);
          if (decimalVal == null) {
            statement.setNull(count, Types.DOUBLE);
          } else {
            statement.setBigDecimal(count, decimalVal);
          }
          break;
        default:
          throw new UnsupportedOperationException("Unable to handle data type " + columnSchema.getType());
      }
      count++;
    }
  }

  public void loadData(final Optional<Integer> numRowsOverrideOption) {
    long startTime = System.currentTimeMillis();

    long startTimestamp;
    long endTimestamp = scenarioStartTimestamp;
    long delta = (scenarioEndTimestamp - scenarioStartTimestamp)/threadPoolSize;
    List<CalciteKuduTable> cubeTables = calciteKuduTable.getCubeTables();
    int numRows = numRowsOverrideOption.orElseGet(scenario::getNumRows);
    for (int t = 0; t < threadPoolSize; ++t) {
      long floorMod = cubeTables.isEmpty() ? DateTimeUtils.MILLIS_PER_SECOND : cubeGranularityFloorMod;
      // for each thread the start time stamp is the previous threads end timestamp (other than
      // the first thread for which we set it to the scenario min timestamp)
      startTimestamp  = endTimestamp;
      // for each thread the end timestamp is set to the floor of the (start timestamp + delta) to
      // DAY so that cubes are aggregated correctly (other than the last thread for which we set
      // it to the scenario max timestamp)
      boolean lastThread = t == threadPoolSize - 1;
      endTimestamp = lastThread ? scenarioEndTimestamp :
        SqlFunctions.floor(startTimestamp + delta, floorMod);

      long finalStartTimestamp = startTimestamp;
      long finalEndTimestamp = endTimestamp;
      // callable implemented as lambda expression
      Callable<Void> callableObj = () -> {
        logger.info("startTimestamp {}", new Date(finalStartTimestamp));
        logger.info("endTimestamp {}", new Date(finalEndTimestamp));
        int numRowsWrittenPerThread = numRows / threadPoolSize;
        if (lastThread) {
          // let the last thread write any remaining rows
          numRowsWrittenPerThread += numRows % threadPoolSize;
        }
        long threadStartTime = System.currentTimeMillis();
        // create a timestamp column value generator that generates timestamps for this subset
        // of time ranges
        UniformLongValueGenerator subsetTimestampGenerator =
          new UniformLongValueGenerator(finalStartTimestamp, finalEndTimestamp);
        try (Connection conn = DriverManager.getConnection(url)) {
          // Create prepared statement that can be reused
          String sql = buildSql();
          PreparedStatement stmt = conn.prepareStatement(sql);

          // populate table with data
          for (int i = 1; i <= numRowsWrittenPerThread; ++i) {
            bindValues(stmt, subsetTimestampGenerator);
            stmt.execute();
            if (i % batchSize == 0) {
              conn.commit();
              logger.info("Total number of rows committed {} time taken for " +
                "current batch {}", i, (System.currentTimeMillis() - threadStartTime));
              threadStartTime = System.currentTimeMillis();
            }
          }
          conn.commit();
        }
        logger.info("Total number of rows committed {} time taken {}", numRowsWrittenPerThread,
          (System.currentTimeMillis() - threadStartTime));
        return null;
      };
      completionService.submit(callableObj);
    }

    for (int i = 0; i < threadPoolSize; i++) {
      try {
        Future<Void> result = completionService.take();
        result.get();
      } catch (Exception e) {
        logger.error("Got an exception while writing", e);
      }
    }
    logger.info("Total number of rows committed {} time taken {} ", numRows,
      (System.currentTimeMillis() - startTime));
    threadPool.shutdown();
  }

}

