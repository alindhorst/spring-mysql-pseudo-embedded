package de.alexanderlindhorst.spring.jdbc.embedded.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

/**
 *
 * @author alindhorst
 */
public class EmbeddedMySQLDataSourceFactoryTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedMySQLDataSourceFactoryTest.class);

    /**
     * Test of getConnectionProperties method, of class EmbeddedMySQLDataSourceFactory.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void checkGetConnectionPropertiesThrowsException() {
        new EmbeddedMySQLDataSourceFactory().getConnectionProperties();
        fail("The test should have thrown an exception one step earlier");
    }

    /**
     * Test of getDataSource method, of class EmbeddedMySQLDataSourceFactory.
     */
    @Test
    public void checkGetDataSourceFiresUpDatabase() throws SQLException {
        EmbeddedMySQLDataSourceFactory instance = new EmbeddedMySQLDataSourceFactory();
        DataSource dataSource = instance.getDataSource();
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT version;");
        String version = null;
        while (resultSet.next()) {
            version = resultSet.getString(1);
            LOGGER.info("Found version {}", version);
        }
        resultSet.close();
        statement.close();
        connection.close();
        assertThat(version, is(not(nullValue())));
    }
}