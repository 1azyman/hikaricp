package com.evolveum.hikaricp;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PoolTest {

    private static final Logger LOG = LoggerFactory.getLogger(PoolTest.class);

    private static final String JDBC_URL = "jdbc:oracle:thin:@localhost:1521:XE";
    private static final String JDBC_USERNAME = "midtest410";
    private static final String JDBC_PASSWORD = "midtest410";

    @Test(timeOut = 10_000L)
    public void exampleTest() throws Exception {
        HikariDataSource dataSource = createDataSource();
        try {
            MyRunnable runnable = new MyRunnable(dataSource);
            Thread thread = new Thread(runnable);
            thread.start();

            makeQuery(dataSource, "Doing some work on main thread");

            Thread.sleep(2_000L);

            LOG.info("Interrupting other thread");
            thread.interrupt();
            LOG.info("Interrupted other thread");

            makeQuery(dataSource, "Doing some work on main thread after other thread was interrupted");

            Thread.sleep(500L);

            Assert.assertNull(runnable.getThrowable(), "Exception occurred in other thread");
        } finally {
            dataSource.close();
        }
    }

    private HikariDataSource createDataSource() {
        HikariConfig config = createHikariConfig();
        return new HikariDataSource(config);
    }

    private HikariConfig createHikariConfig() {
        HikariConfig config = new HikariConfig();

        config.setDriverClassName(oracle.jdbc.OracleDriver.class.getName());
        config.setJdbcUrl(JDBC_URL);
        config.setUsername(JDBC_USERNAME);
        config.setPassword(JDBC_PASSWORD);

        config.setRegisterMbeans(true);

        config.setMinimumIdle(8);
        config.setMaximumPoolSize(20);

        config.setMaxLifetime(1_800_000);
        config.setIdleTimeout(600_000);
        config.setKeepaliveTime(120_000);

        config.setIsolateInternalQueries(true);
        config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");

        config.setInitializationFailTimeout(1);
        config.setAutoCommit(false);

        return config;
    }

    private static class MyRunnable implements Runnable {

        private final HikariDataSource dataSource;

        private Throwable throwable;

        public MyRunnable(HikariDataSource dataSource) {
            this.dataSource = dataSource;
        }

        public Throwable getThrowable() {
            return throwable;
        }

        @Override
        public void run() {
            try {
                try {
                    while (true) {
                        makeQuery(dataSource, "Doing some work on other thread");

                        Thread.sleep(500L);
                    }
                } catch (InterruptedException ex) {
                    LOG.error("Interrupted", ex);

                    // TODO this is causing the issue
                    //  -> see MockTaskHandler.run() -> MiscUtil.sleepNonInterruptibly(long) -> Thread.currentThread().interrupt();
                      Thread.currentThread().interrupt();
                }

                makeQuery(dataSource, "Doing some work on other thread after it was interrupted");
            } catch (Throwable t) {
                throwable = t;
            }
        }
    }

    private static void makeQuery(HikariDataSource dataSource, String msg) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement("select 123 from dual");
            ResultSet result = stmt.executeQuery();
            result.next();

            Long value = result.getLong(1);
            LOG.info("{}: {}", msg, value);

            result.close();
            stmt.close();

            connection.commit();
        } catch (SQLException ex) {
            throw new RuntimeException("Error while making query, this shouldn't happen", ex);
        }
    }
}
