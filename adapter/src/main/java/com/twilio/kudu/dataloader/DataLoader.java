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
package com.twilio.kudu.dataloader;

import com.twilio.kudu.dataloader.generator.ColumnValueGenerator;
import com.twilio.kudu.dataloader.generator.MultipleColumnValueGenerator;
import com.twilio.kudu.dataloader.generator.UniformLongValueGenerator;
import com.twilio.kudu.sql.CalciteModifiableKuduTable;
import com.twilio.kudu.sql.schema.BaseKuduSchemaFactory;
import org.apache.calcite.avatica.util.DateTimeUtils;
import com.twilio.kudu.sql.CalciteKuduTable;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.KuduCalciteConnectionImpl;
import org.apache.calcite.jdbc.KuduMetaImpl;
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
  private final int COMMIT_BATCH_SIZE = 1000;
  // limit the amount of state that is tracked in CubeMutationState
  private final int CUBE_MUTATION_SIZE = 100000;
  private final long scenarioStartTimestamp;
  private final long scenarioEndTimestamp;
  private final long cubeGranularityFloorMod;

  public DataLoader(final String url, final Scenario scenario) throws SQLException {
    this(url, scenario, null);
  }

  public DataLoader(final String url, final Scenario scenario, Integer threadPoolSize) throws SQLException {
    // load all the CalciteKuduTables
    CalciteConnection connection = DriverManager.getConnection(url).unwrap(CalciteConnection.class);
    BaseKuduSchemaFactory schemaFactory = connection.config().schemaFactory(BaseKuduSchemaFactory.class, null);
    this.scenario = scenario;
    this.calciteKuduTable = schemaFactory.getTable(scenario.getTableName())
        .orElseThrow(() -> new RuntimeException("Table not found " + scenario.getTableName()));
    this.url = url;

    // we assume the second column is the timestamp column
    String timestampColumnName = calciteKuduTable.getKuduTable().getSchema().getColumnByIndex(1).getName();
    UniformLongValueGenerator timestampGenerator = (UniformLongValueGenerator) scenario.getColumnNameToValueGenerator()
        .get(timestampColumnName);
    // initialize minValue and maxValue if the generator is a TimestampGenerator
    timestampGenerator.getColumnValue();
    this.scenarioStartTimestamp = timestampGenerator.minValue;
    this.scenarioEndTimestamp = timestampGenerator.maxValue;
    // Use the largest cube granularity to determine the total number of tasks that
    // can be
    // parallelized. Eg. if the scenario time range is 14 days and we have 1 hourly
    // and 1 daily
    // cube, we can run 14 tasks in parallel (one for each day).
    this.cubeGranularityFloorMod = getMaxCubeGranularity();
    int numTasks = (int) Math.ceil((float) (scenarioEndTimestamp - scenarioStartTimestamp) / cubeGranularityFloorMod);

    // set thread pool to MIN(2 * number of cpus, numTasks) by default
    this.threadPoolSize = Math
        .min(threadPoolSize != null ? threadPoolSize : Runtime.getRuntime().availableProcessors() * 10, numTasks);
    logger.info("Thread pool size {}", this.threadPoolSize);
    this.threadPool = Executors.newFixedThreadPool(this.threadPoolSize, new ThreadFactory() {
      int threadCounter = 0;

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, String.format("DataLoader-%s-%d", scenario.getTableName(), threadCounter++));
      }
    });
    this.completionService = new ExecutorCompletionService<>(threadPool);
  }

  /**
   * @return If there are cube tables, return the largest granularity of the cubes
   *         or else just use the smallest granularity
   */
  private Long getMaxCubeGranularity() {
    Optional<Long> option = calciteKuduTable.getCubeTables().stream()
        .map(t -> ((CalciteModifiableKuduTable) t).getCubeMaintainer().getFloorMod()).max(Long::compare);
    return option.orElse(DateTimeUtils.MILLIS_PER_SECOND);
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
      } else {
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

  private void bindValues(PreparedStatement statement, UniformLongValueGenerator timestampGenerator)
      throws SQLException {
    if (scenario.getMultipleColumnValueGenerators() != null) {
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
        } else {
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
        } else {
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
        // If we are using multiple threads have each thread write data for a non
        // overlapping
        // time range. We assume the second column is the date partitioned columnLong
        // longValue =
        Long timestampValue;
        if (count == 2 && threadPoolSize > 1) {
          timestampValue = timestampGenerator.getColumnValue();
        } else {
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
    logger.info("scenario start timestamp {} end timestamp {}", new Date(scenarioStartTimestamp),
        new Date(scenarioEndTimestamp));
    long startTime = System.currentTimeMillis();

    long prevThreadEndTimestamp = scenarioStartTimestamp;
    long rangePerThread = (scenarioEndTimestamp - scenarioStartTimestamp) / threadPoolSize;
    long threadDelta = Math.max(rangePerThread, cubeGranularityFloorMod);
    int numRows = numRowsOverrideOption.orElseGet(scenario::getNumRows);
    for (int t = 0; t < threadPoolSize; ++t) {
      // for each thread the start time stamp is the previous threads end timestamp
      // (except the first thread for which we set it to the scenario min timestamp)
      // for each thread the end timestamp is set to the floor of the (start timestamp
      // + delta)
      // truncated to the max cube granularity DAY so that cubes are aggregated
      // correctly
      // (except the last thread for which we set it to the scenario max timestamp)
      boolean lastThread = t == threadPoolSize - 1;
      final long threadStartTimestamp = prevThreadEndTimestamp;
      final long threadEndTimestamp = lastThread ? scenarioEndTimestamp
          : SqlFunctions.floor(threadStartTimestamp + threadDelta, cubeGranularityFloorMod);

      // further divide the time range that each thread is responsible for so that we
      // limit
      // the amount of state in CubeMutationState
      int numTasksPerThread = (int) Math
          .ceil((float) (threadEndTimestamp - threadStartTimestamp) / cubeGranularityFloorMod);

      // callable implemented as lambda expression
      final int threadIndex = t;
      Callable<Void> callableObj = () -> {
        logger.info("thread{} start timestamp {}", threadIndex, new Date(threadStartTimestamp));
        logger.info("thread{} end timestamp {}", threadIndex, new Date(threadEndTimestamp));
        int numRowsPerThread = numRows / threadPoolSize;
        if (lastThread) {
          // let the last thread write any remaining rows
          numRowsPerThread += numRows % threadPoolSize;
        }
        int maxBatchesPerThread = (int) Math.ceil((float) (numRowsPerThread) / CUBE_MUTATION_SIZE);
        int numCubeMutationBatches = Math.min(numTasksPerThread, maxBatchesPerThread);
        logger.info("Number of mutation batches per thread {}", numCubeMutationBatches);
        long threadStartTime = System.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(url)) {
          // Create prepared statement that can be reused
          String sql = buildSql();
          PreparedStatement stmt = conn.prepareStatement(sql);
          // populate table with data
          int rowCount = 0;
          long batchStartTimestamp;
          long batchEndTimestamp = threadStartTimestamp;
          for (int i = 1; i <= numCubeMutationBatches; ++i) {
            long rangePerBatch = (threadEndTimestamp - threadStartTimestamp) / numCubeMutationBatches;
            long cubeDelta = Math.max(rangePerBatch, cubeGranularityFloorMod);
            batchStartTimestamp = batchEndTimestamp;
            batchEndTimestamp = i == numCubeMutationBatches ? threadEndTimestamp
                : SqlFunctions.floor(batchStartTimestamp + cubeDelta, cubeGranularityFloorMod);
            logger.info("batch {} start timestamp {}", i, new Date(batchStartTimestamp));
            logger.info("batch {} end timestamp {}", i, new Date(batchEndTimestamp));
            // create a timestamp column value generator that generates timestamps for this
            // subset of time ranges
            UniformLongValueGenerator subsetTimestampGenerator = new UniformLongValueGenerator(batchStartTimestamp,
                batchEndTimestamp);
            int numRowsInBatch = numRowsPerThread / numCubeMutationBatches;
            // add any remaining rows to the last batch
            if (i == numCubeMutationBatches) {
              numRowsInBatch += numRowsPerThread % numCubeMutationBatches;
            }
            for (int j = 1; j <= numRowsInBatch; ++j) {
              bindValues(stmt, subsetTimestampGenerator);
              stmt.execute();
              if (++rowCount % COMMIT_BATCH_SIZE == 0) {
                conn.commit();
                logger.info("Total number of rows committed {} time taken {}", rowCount,
                    (System.currentTimeMillis() - threadStartTime));
                threadStartTime = System.currentTimeMillis();
              }
            }
            // commit any remaining rows
            conn.commit();
            KuduMetaImpl kuduMetaImpl = (conn.unwrap(KuduCalciteConnectionImpl.class)).getMeta();
            kuduMetaImpl.clearMutationState();
          }
        }
        logger.info("Total number of rows committed {} time taken {}", numRowsPerThread,
            (System.currentTimeMillis() - threadStartTime));
        return null;
      };
      completionService.submit(callableObj);
      prevThreadEndTimestamp = threadEndTimestamp;
    }

    for (int i = 0; i < threadPoolSize; i++) {
      try {
        Future<Void> result = completionService.take();
        result.get();
      } catch (Exception e) {
        logger.error("Got an exception while writing", e);
      }
    }
    logger.info("Total number of rows committed {} time taken {} ", numRows, (System.currentTimeMillis() - startTime));
    threadPool.shutdown();
  }

}
