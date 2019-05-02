package com.twilio.raas.sql;

import com.zaxxer.hikari.HikariDataSource;
import java.util.concurrent.CompletionStage;
import java.sql.ResultSet;
import java.util.List;
import java.util.function.Function;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CompletableFuture;
import java.sql.Connection;
import java.sql.PreparedStatement;
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
    public static String CALCITE_MODEL_TEMPLATE = "jdbc:calcite:model=inline:{version: '1.0',defaultSchema:'kudu',schemas:[{name: 'kudu',type:'custom',factory:'com.twilio.raas.sql.KuduSchemaFactory',operand:{connect:'%s'}}]};caseSensitive=false";
    private static int POOL_COUNTER = 0;
  
    private HikariDataSource dbPool;
    private ExecutorService threadPool;
    private String jdbcUrl;

    /**
     * Create a runner with a kudu connection string in the format of host1:7051,host2:7051
     * where 7051 is the rpc port of the Kudu Leader running on that host.
     *
     * @param kuduConnectionString comma delimited list of kudu master and rpc port
     * @param threadPoolSize       size of the jdbc connection pool
     */
    public JDBCQueryRunner(final String kuduConnectionString, final int threadPoolSize) {
        this(kuduConnectionString, threadPoolSize, null);
    }

    /**
     * Create a runner with a kudu connection string in the format of host1:7051,host2:7051
     * where 7051 is the rpc port of the Kudu Leader running on that host.
     *
     * @param kuduConnectionString comma delimited list of kudu master and rpc port
     * @param threadPoolSize       size of the jdbc connection pool
     * @param registry             Dropwizard metrics registry that will be used for some stats.
     */
    public JDBCQueryRunner(final String kuduConnectionString, final int threadPoolSize, final MetricRegistry registry) {
        final Properties connectionProps = new Properties();

        this.jdbcUrl = String.format(CALCITE_MODEL_TEMPLATE, kuduConnectionString);
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
     * @param sql                   Raw sql to execute against dataset
     * @param resultSetTransformer  function to transform a {@link ResultSet} into a T
     * 
     * @return Future that will contain a {@link List} of T
     */
    public <T> CompletionStage<List<T>> executeSql(final String sql, final Function<ResultSet, T> resultSetTransformer) {
        final CompletableFuture<List<T>> pendingResult = new CompletableFuture<>();
        this.threadPool.execute(() -> {
                List<T> allRows = new ArrayList<>();
                try (Connection con = this.dbPool.getConnection();
                     Statement statement = con.createStatement();
                     ResultSet rs = statement.executeQuery(sql)) {
                    while(rs.next()) {
                        allRows.add(resultSetTransformer.apply(rs));
                    }
                    pendingResult.complete(allRows);
                }
                catch (Exception failure) {
                    pendingResult.completeExceptionally(failure);
                }
            });
        return pendingResult;
    }

    @Override
    public void close() throws Exception {
        this.dbPool.close();
        this.threadPool.shutdown();
    }
}
