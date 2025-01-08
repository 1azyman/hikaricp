package com.evolveum.hikaricp;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import oracle.jdbc.OracleDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.*;

public class PoolTest {

    private static final Logger LOG = LoggerFactory.getLogger(PoolTest.class);

    @Test(timeOut = 10_000L)
    public void testOracle() throws Exception {
        testThreadInterruption(OracleDriver.class, "jdbc:oracle:thin:@localhost:1521:XE", "midtest410", "midtest410", "select 123 from dual");
    }

    @Test(timeOut = 10_000L)
    public void testPostgreSQL() throws Exception {
        testThreadInterruption(org.postgresql.Driver.class, "jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres", "select 123");
    }

    public void testThreadInterruption
            (Class<? extends Driver> driver, String jdbcUrl, String jdbcUsername, String jdbcPassword, String query) throws Exception {

        HikariDataSource dataSource = createDataSource(driver, jdbcUrl, jdbcUsername, jdbcPassword);
        try {
            MyRunnable runnable = new MyRunnable(dataSource, query);
            Thread thread = new Thread(runnable);
            thread.start();

            makeQuery(dataSource, query, "Doing some work on main thread");

            Thread.sleep(2_000L);

            LOG.info("Interrupting other thread");
            thread.interrupt();
            LOG.info("Interrupted other thread");

            makeQuery(dataSource, query, "Doing some work on main thread after other thread was interrupted");

            Thread.sleep(500L);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(bos);
            if (runnable.getThrowable() != null) {
                runnable.getThrowable().printStackTrace(ps);
            }
            Assert.assertNull(runnable.getThrowable(), "Exception occurred in other thread\n" + bos);
        } finally {
            dataSource.close();
        }
    }

    private HikariDataSource createDataSource(
            Class<? extends Driver> driver, String jdbcUrl, String jdbcUsername, String jdbcPassword) {
        HikariConfig config = createHikariConfig(driver, jdbcUrl, jdbcUsername, jdbcPassword);
        return new HikariDataSource(config);
    }

    private HikariConfig createHikariConfig(
            Class<? extends Driver> driver, String jdbcUrl, String jdbcUsername, String jdbcPassword) {

        HikariConfig config = new HikariConfig();

        config.setDriverClassName(driver.getName());
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(jdbcUsername);
        config.setPassword(jdbcPassword);

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

        private final String query;

        private Throwable throwable;

        public MyRunnable(HikariDataSource dataSource, String query) {
            this.dataSource = dataSource;
            this.query = query;
        }

        public Throwable getThrowable() {
            return throwable;
        }

        @Override
        public void run() {
            try {// (Connection connection = dataSource.getConnection()) {
                try {
                    while (true) {
                        makeQuery(dataSource, query, "Doing some work on other thread");

                        Thread.sleep(500L);
                    }
                } catch (InterruptedException ex) {
                    LOG.error("Interrupted", ex);

                    // TODO this is causing the issue
                    //  -> see MockTaskHandler.run() -> MiscUtil.sleepNonInterruptibly(long) -> Thread.currentThread().interrupt();
                    Thread.currentThread().interrupt();
                }

//                LOG.info("Connection valid: {} and closed: {}", connection.isValid(1_000), connection.isClosed());

                makeQuery(dataSource, query, "Doing some work on other thread after it was interrupted");
            } catch (Throwable t) {
                throwable = t;
            }
        }
    }

    private static void makeQuery(HikariDataSource dataSource, String query, String msg) {
        try (Connection connection = dataSource.getConnection()) {
            PreparedStatement stmt = connection.prepareStatement(query);
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
