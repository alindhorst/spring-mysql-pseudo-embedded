package de.alexanderlindhorst.spring.jdbc.embedded.mysql;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.mysql.management.MysqldResource;

/**
 *
 * @author alindhorst
 */
class EmbeddedMySQLDataSource implements DataSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedMySQLDataSource.class);
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL_TEMPLATE = "jdbc:mysql://localhost:%d/embeddedmysql";
    private MysqldResource dbServer;
    private Map<String, String> options;
    private PrintWriter logWriter;

    public EmbeddedMySQLDataSource(MysqldResource dbServer, Map<String, String> options) {
        if (dbServer == null || options == null) {
            throw new NullPointerException();
        }
        this.dbServer = dbServer;
        this.options = options;
        logWriter = new PrintWriter(new DefaultLogWriter());
    }

    @Override
    public Connection getConnection() throws SQLException {
        startDB();
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException ex) {
            throw new SQLException("Couldn't load driver", ex);
        }
        String url = String.format(URL_TEMPLATE, getPort());
        return DriverManager.getConnection(url);
    }

    @Override
    public Connection getConnection(String string, String string1) throws SQLException {
        throw new UnsupportedOperationException("Not supported, user and password are fixed");
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    @Override
    public void setLogWriter(PrintWriter writer) throws SQLException {
        logWriter = writer;
    }

    @Override
    public void setLoginTimeout(int i) throws SQLException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return false;
    }

    public synchronized void startDB() {
        if (!dbServer.isRunning()) {
            dbServer.start("embedded-thread", options);
        }
        if (!dbServer.isRunning()) {
            throw new IllegalStateException();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                stopDB();
            }
        }));
        LOGGER.info("embedded MySQL instance started on port {}", getPort());
    }

    public void stopDB() {
        if (dbServer.isRunning()) {
            dbServer.shutdown();
        }
    }

    private int getPort() {
        String portString = options.get(MysqldResource.PORT);
        if (Strings.isNullOrEmpty(portString)) {
            throw new NullPointerException("port number not set");
        }
        return Integer.valueOf(portString).intValue();
    }

    private static class DefaultLogWriter extends Writer {

        private StringBuilder builder = new StringBuilder();

        @Override
        public void write(char[] chars, int i, int i1) throws IOException {
            builder.append(chars, i, i1);
        }

        @Override
        public void flush() throws IOException {
            LOGGER.debug(builder.toString());
            builder = new StringBuilder();
        }

        @Override
        public void close() throws IOException {
            flush();
        }
    }
}
