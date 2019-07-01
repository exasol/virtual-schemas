package com.exasol.adapter.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

import com.exasol.*;
import com.exasol.adapter.AdapterProperties;
import com.exasol.auth.kerberos.KerberosConfigurationCreator;

/**
 * Factory that produces JDBC connections to remote data sources.
 */
public class RemoteConnectionFactory {
    private static final Logger LOGGER = Logger.getLogger(RemoteConnectionFactory.class.getName());

    /**
     * Create a JDBC connection to the remote data source.
     *
     * @param exaMetadata Exasol metadata (contains information about stored * connection details)
     * @param properties  user-defined adapter properties
     * @return JDBC connection to remote data source
     * @throws SQLException if the connection to the remote source could not be established
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

    protected void logConnectionAttempt(final String address, final String username) {
        LOGGER.fine(
                () -> "Connecting to \"" + address + "\" as user \"" + username + "\" using password authentication.");
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
            final String password = exaConnection.getPassword();
            final String username = exaConnection.getUser();
            final String address = exaConnection.getAddress();
            if (KerberosConfigurationCreator.isKerberosAuthentication(password)) {
                return establishConnectionWithKerberos(password, username, address);
            } else {
                return establishConnectionWithRegularCredentials(password, username, address);
            }
        } catch (final ExaConnectionAccessException exception) {
            throw new RemoteConnectionException(
                    "Could not access the connection information of connection \"" + connectionName + "\".", exception);
        }
    }

    private Connection establishConnectionWithKerberos(final String password, final String username,
            final String address) throws SQLException {
        logConnectionAttemptWithKerberos(address, username);
        final Properties jdbcProperties = new Properties();
        jdbcProperties.put("user", username);
        jdbcProperties.put("password", password);
        final KerberosConfigurationCreator kerberosConfigurationCreator = new KerberosConfigurationCreator();
        kerberosConfigurationCreator.writeKerberosConfigurationFiles(username, password);
        final Connection connection = DriverManager.getConnection(address, jdbcProperties);
        logRemoteDatabaseDetails(connection);
        return connection;
    }

    private void logConnectionAttemptWithKerberos(final String address, final String username) {
        LOGGER.fine(
                () -> "Connecting to \"" + address + "\" as user \"" + username + "\" using Kerberos authentication.");
    }

    private Connection establishConnectionWithRegularCredentials(final String password, final String username,
            final String address) throws SQLException {
        logConnectionAttempt(address, username);
        final Connection connection = DriverManager.getConnection(address, username, password);
        logRemoteDatabaseDetails(connection);
        return connection;
    }
}
