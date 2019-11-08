package com.twilio.raas.sql;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.calcite.runtime.Hook;
import com.twilio.dataEngine.protocol.ExecuteQueryLog;
import java.util.function.Consumer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.linq4j.tree.Expression;

import java.sql.SQLException;
import java.util.concurrent.CompletionStage;
import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Properties;
import com.zaxxer.hikari.HikariConfig;
import java.util.concurrent.ThreadFactory;
import java.sql.Statement;
import com.codahale.metrics.MetricRegistry;

/**
 * Executes Sql queries asynchronously. By providing a thread pool size,
 * the code will create a set of jdbc connections equal to that size and
 * issue sql queries over those connections.
 */
public final class JDBCQueryRunner implements AutoCloseable {
    public static String CALCITE_MODEL_TEMPLATE = "jdbc:calcite:model=inline:{version: '1.0',defaultSchema:'kudu',schemas:[{name: 'kudu',type:'custom',factory:'com.twilio.raas.sql.KuduSchemaFactory',operand:{connect:'%s',kuduTableConfigs:[{tableName: 'ReportCenter.AuditEvents', descendingSortedFields:['event_date']}, {tableName: 'AuditEvents-DailyIndex-Aggregation', descendingSortedFields:['event_date']}]}}]};caseSensitive=false;timeZone=UTC";
    private static int POOL_COUNTER = 0;

    private HikariDataSource dbPool;
    private ExecutorService threadPool;
    private final String jdbcUrl;

    /**
     * Overloaded constructor to create a runner with a kudu connection string in the format of host1:7051,host2:7051
     * where 7051 is the rpc port of the Kudu Leader running on that host.
     *
     * @param kuduConnectionString comma delimited list of kudu master and rpc port
     * @param threadPoolSize       size of the jdbc connection pool
     */
    public JDBCQueryRunner(final String kuduConnectionString, final int threadPoolSize) {
        this(CALCITE_MODEL_TEMPLATE, kuduConnectionString, threadPoolSize, null);
    }

    /**
     * Overloaded constructor which allows a yaml template to be specified for query processing
     *
     * @param template yaml template for defining schemata and other DB, table configs
     * @param kuduConnectionString comma delimited list of kudu master and rpc port
     * @param threadPoolSize       size of the jdbc connection pool
     */
    public JDBCQueryRunner(final String template, final String kuduConnectionString, final int threadPoolSize) {
        this(template, kuduConnectionString, threadPoolSize, null);
    }

    /**
     * Overloaded constructor with metrics registry parameter
     *
     * @param kuduConnectionString comma delimited list of kudu master and rpc port
     * @param threadPoolSize       size of the jdbc connection pool
     * @param registry MetricRegistry to capture metrics around query processing
     */
    public JDBCQueryRunner(final String kuduConnectionString, final int threadPoolSize, final MetricRegistry registry) {
        this(CALCITE_MODEL_TEMPLATE, kuduConnectionString, threadPoolSize, registry);
    }

    /**
     * Create a runner with a kudu connection string in the format of host1:7051,host2:7051
     * where 7051 is the rpc port of the Kudu Leader running on that host.
     *
     * @param template yaml template for defining schemata and other DB, table configs
     * @param kuduConnectionString comma delimited list of kudu master and rpc port
     * @param threadPoolSize       size of the jdbc connection pool
     * @param registry             Dropwizard metrics registry that will be used for some stats.
     */
    public JDBCQueryRunner(final String template, final String kuduConnectionString, final int threadPoolSize, final MetricRegistry registry) {
        final Properties connectionProps = new Properties();

        this.jdbcUrl = String.format(template, kuduConnectionString);
        connectionProps.put("jdbcUrl", jdbcUrl);
        connectionProps.put("maximumPoolSize", threadPoolSize);
        connectionProps.put("minimumIdle", threadPoolSize);
        connectionProps.put("connectionInitSql", "select 1");
        connectionProps.put("maxLifetime", 0);
        this.dbPool = new HikariDataSource(new HikariConfig(connectionProps));
        if (registry != null) {
            this.dbPool.setMetricRegistry(registry);
        }

        // Create a thread factory that identifies the thread with a nice name.
        this.threadPool = Executors.newFixedThreadPool(threadPoolSize, new ThreadFactory() {
                final int poolId = POOL_COUNTER + 1;
                int threadCounter = 0;
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, String.format("JDBCRunner-Pool-%d-%d",
                                                       poolId, ++threadCounter));
                }
            });
    }

    /**
     * Executes the SQL string against the database in a Thread. Returns a
     * future that will be complete with either a list of T or with an exception
     *
     * @param <T>                   A user supplied type that a {@link ResultSet} will be transformed into
     * @param sql                   Raw sql to execute against data set
     * @param resultSetTransformer  function to transform a {@link ResultSet} into a T
     *
     * @return Future that will contain a {@link List} of T
     */
    public <T> CompletionStage<List<T>> executeSql(final String sql, final Function<ResultSet, T> resultSetTransformer,
        final ExecuteQueryLog log, final Boolean isPrevious) {

        final CompletableFuture<List<T>> pendingResult = new CompletableFuture<>();
        final long taskCreationTime = System.currentTimeMillis();
        this.threadPool.execute(() -> {
                List<T> allRows = new ArrayList<>();
                try (Connection con = this.dbPool.getConnection()) {
                    final long connectionAcquiredAt = System.currentTimeMillis();
                    log.setJdbcConnectionAcquired(isPrevious, connectionAcquiredAt - taskCreationTime);
                    try (Statement statement = con.createStatement();
                        ResultSet rs = statement.executeQuery(sql)) {
                        boolean firstRow = true;
                        while(rs.next()) {
                            if (firstRow) {
                                firstRow = false;
                                log.setDurationToFirstRow(isPrevious,
                                    System.currentTimeMillis() - connectionAcquiredAt);
                            }
                            allRows.add(resultSetTransformer.apply(rs));
                        }
                        pendingResult.complete(allRows);
                    }
                }
                catch (Exception | Error failure) {
                    pendingResult.completeExceptionally(failure);
                }
            });
        return pendingResult;
    }

    /**
     * @return  Calcite's plan for the given sql
     */
    // TODO see if there is a better way to represent the query plan instead of a String
    public String getExplainPlan(final String sql) throws SQLException {
        try (Connection con = this.dbPool.getConnection();
             Statement statement = con.createStatement();
             ResultSet rs = statement.executeQuery("EXPLAIN PLAN FOR " + sql)) {
             return SqlUtil.getExplainPlan(rs);
        }
    }

    @Override
    public void close() throws Exception {
        this.dbPool.close();
        this.threadPool.shutdown();
    }

    /**
     * Remember to close this connection
     */
    public Connection getConnection() throws SQLException {
        return this.dbPool.getConnection();
    }
}
