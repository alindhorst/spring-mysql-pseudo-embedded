package de.alexanderlindhorst.spring.jdbc.embedded.mysql;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.mysql.management.driverlaunched.ServerLauncherSocketFactory;

/**
 *
 * @author alindhorst
 */
class EmbeddedMySQLDataSource implements DataSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedMySQLDataSource.class);
    private static final String USER = "pseudo";
    private static final String PASSWORD = "embedded";
    private static final String DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL_TEMPLATE = "jdbc:mysql:mxj://localhost:%d/embeddedmysql?server.basedir=%s"
            + "&createDatabaseIfNotExist=true"
            + "&server.initialize-user=true";
    private PrintWriter logWriter;
    private String url;
    private AtomicBoolean initialized = new AtomicBoolean(false);
    private File serverPath;

    public EmbeddedMySQLDataSource(int port, String dbServerPath) {
        if (Strings.isNullOrEmpty(dbServerPath)) {
            throw new NullPointerException();
        }
        serverPath = new File(dbServerPath);
        url = String.format(URL_TEMPLATE, port, dbServerPath);
        logWriter = new PrintWriter(new DefaultLogWriter());
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException ex) {
            throw new SQLException("Couldn't load driver", ex);
        }
        if (!initialized.get()) {
            initialized.set(true);
            LOGGER.debug("DataSource not fired up yet, will register shutdown hook");
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    shutdown();
                }
            }));
        }
        LOGGER.debug("Opening connection to url {}", url);
        return DriverManager.getConnection(url, USER, PASSWORD);
    }

    @Override
    public Connection getConnection(String string, String string1) throws SQLException {
        LOGGER.debug("getConnection called with credentials, will be ignored");
        return getConnection();
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
        LOGGER.debug("Asked if is wrapper for ", type);
        return false;
    }

    public void shutdown() {
        ServerLauncherSocketFactory.shutdown(serverPath, null);
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
