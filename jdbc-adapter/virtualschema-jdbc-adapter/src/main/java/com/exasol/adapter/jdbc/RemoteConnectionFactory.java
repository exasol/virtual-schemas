package com.exasol.adapter.jdbc;

import java.sql.*;
import java.util.logging.Logger;

import com.exasol.*;
import com.exasol.adapter.AdapterProperties;

public class RemoteConnectionFactory {
    private static final Logger LOGGER = Logger.getLogger(RemoteConnectionFactory.class.getName());

    public Connection createConnection(ExaMetadata exaMetadata, AdapterProperties properties) throws SQLException {
        final String connectionName = properties.getConnectionName();
        Connection connection;
        String username;
        String password;

        if ((connectionName != null) && !connectionName.isEmpty()) {
            try {
                ExaConnectionInformation exaConnection = exaMetadata.getConnection(connectionName);
                username = exaConnection.getUser();
                password = exaConnection.getPassword();
                logConnectionAttempt(username, password);
                connection = DriverManager.getConnection(exaConnection.getAddress(), username, password);
            } catch (final ExaConnectionAccessException exception) {
                throw new RuntimeException(
                        "Could not access the connection information of connection \"" + connectionName + "\"",
                        exception);
            }
        } else {
            username = properties.getUsername();
            password = properties.getPassword();
            logConnectionAttempt(username, password);
            connection = DriverManager.getConnection(properties.getConnectionString(), username, password);
        }
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
}
