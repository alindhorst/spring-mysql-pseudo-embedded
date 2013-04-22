package de.alexanderlindhorst.spring.jdbc.embedded.mysql;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Driver;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.embedded.ConnectionProperties;
import org.springframework.jdbc.datasource.embedded.DataSourceFactory;

/**
 * Fires up a pseudo embedded MySQL instance and provides {@link DataSource} instances to access it
 *
 * @author alindhorst
 */
public class EmbeddedMySQLDataSourceFactory implements DataSourceFactory {

    private static final int AND_MASK = 0xFFFF;
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedMySQLDataSourceFactory.class);
    private static EmbeddedMySQLDataSource dataSource;

    static {
        initInstance();
    }

    private static synchronized void initInstance() {
        if (dataSource != null) {
            throw new IllegalStateException("There is already an instance ... WTF?");
        }
        int port = pickRandomPort(10);
        File dbServerPath = new File(System.getProperty("java.io.tmpdir"), "mysqld-" + port);
        dbServerPath.deleteOnExit();
        dataSource = new EmbeddedMySQLDataSource(port, dbServerPath.getAbsolutePath());
    }

    /**
     * Tries to randomly pick a port
     *
     * @param maxtries
     * @return
     */
    private static int pickRandomPort(int maxtries) {
        if (maxtries == 0) {
            throw new IllegalStateException("Couldn't get unused port ... WTF?");
        }
        int port = (int) ((long) (Math.random() * Long.MAX_VALUE) & AND_MASK);
        if (port < 1024) {
            port += 1024;
        }
        //check if port is already used
        Socket socket = new Socket();
        InetSocketAddress target = null;
        try {
            target = new InetSocketAddress(InetAddress.getLocalHost(), port);
            socket.setSoLinger(false, 0);
            socket.setSoTimeout(100);
        } catch (IOException iOException) {
            LOGGER.error("Couldn't setup socket to check for free port", iOException);
        }
        try {
            socket.connect(target);
            socket.getOutputStream().write(0);
            port = pickRandomPort(--maxtries);
        } catch (IOException iOException) {
            LOGGER.debug("Exception expected here, means that the picked port is unused");
        }
        return port;
    }

    @Override
    public ConnectionProperties getConnectionProperties() {
        return new ConnectionProperties() {
            @Override
            public void setDriverClass(Class<? extends Driver> type) {
                LOGGER.debug("setDriverClass to {} ignored", type);
            }

            @Override
            public void setUrl(String string) {
                LOGGER.debug("setUrl to {}  ignored", string);
            }

            @Override
            public void setUsername(String string) {
                LOGGER.debug("setUsername to {} call ignored", string);
            }

            @Override
            public void setPassword(String string) {
                LOGGER.debug("setPassword to {} ignored", string);
            }
        };
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }
}
