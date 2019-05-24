package com.exasol.adapter.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterProperties;

/**
 * Factory that produces JDBC connections to remote data sources
 */
public class RemoteConnectionFactory {
    private static final Logger LOGGER = Logger.getLogger(RemoteConnectionFactory.class.getName());

    /**
     * Create a JDBC connection to the remote data source
     *
     * @param exaMetadata Exasol metadata (contains information about stored *
     *                    connection details)
     * @param properties  user-defined adapter properties
     * @return JDBC connection to remote data source
     * @throws SQLException if the connection to the remote source could not be
     *                      established
     */
    public Connection createConnection(final ExaMetadata exaMetadata, final AdapterProperties properties)
            throws SQLException {
        final String connectionName = properties.getConnectionName();
        if ((connectionName != null) && !connectionName.isEmpty()) {
            return createConnection(connectionName, exaMetadata);
        } else {
            return createConnectionWithUserCredentials(properties.getUsername(), properties.getPassword(),
                    properties.getConnectionString());
        }
    }

    private Connection createConnectionWithUserCredentials(final String username, final String password,
            final String connectionString) throws SQLException {
        logConnectionAttempt(username, password);
        final Connection connection = DriverManager.getConnection(connectionString, username, password);
        logRemoteDatabaseDetails(connection);
        return connection;
    }

    protected void logConnectionAttempt(final String address, final String user) {
        LOGGER.fine(() -> "Connecting to \"" + address + "\" as user \"" + user + "\"");
    }

    protected void logRemoteDatabaseDetails(final Connection connection) throws SQLException {
        final String databaseProductName = connection.getMetaData().getDatabaseProductName();
        final String databaseProductVersion = connection.getMetaData().getDatabaseProductVersion();
        LOGGER.info(() -> "Connected to " + databaseProductName + " " + databaseProductVersion);
    }

    private Connection createConnection(final String connectionName, final ExaMetadata exaMetadata)
            throws SQLException {
        try {
            final ExaConnectionInformation exaConnection = exaMetadata.getConnection(connectionName);
            final String username = exaConnection.getUser();
            final String address = exaConnection.getAddress();
            logConnectionAttempt(address, username);
            final Connection connection = DriverManager.getConnection(address, username, exaConnection.getPassword());
            logRemoteDatabaseDetails(connection);
            return connection;
        } catch (final ExaConnectionAccessException exception) {
            throw new RemoteConnectionException(
                    "Could not access the connection information of connection \"" + connectionName + "\"", exception);
        }
    }
}
