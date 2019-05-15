package com.exasol.adapter.jdbc;

import java.sql.SQLException;
import java.util.logging.Logger;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.sql.SqlStatement;

/**
 * This class creates <code>IMPORT</code> statements from push-down queries.
 */
public class ImportQueryBuilder {
    private static final Logger LOGGER = Logger.getLogger(ImportQueryBuilder.class.getName());
    private static final String IMPORT_FROM_EXA_PROPERTY = "IMPORT_FROM_EXA";
    private static final String IMPORT_FROM_ORA_PROPERTY = "IMPORT_FROM_ORA";
    private static final String EXA_CONNECTION_STRING_PROPERTY = "EXA_CONNECTION_STRING";
    private static final String ORA_CONNECTION_NAME_PROPERTY = "ORA_CONNECTION_NAME";
    private final ExaMetadata exaMetadata;
    private final SqlDialect dialect;

    /**
     * Create a new instance of an {@link ImportQueryBuilder}
     *
     * @param dialect     SQL dialect
     * @param exaMetadata Exasol metadata
     */
    public ImportQueryBuilder(final SqlDialect dialect, final ExaMetadata exaMetadata) {
        this.dialect = dialect;
        this.exaMetadata = exaMetadata;
    }

    /**
     * Create the <code>IMPORT</code> statement
     *
     * @param statement  SQL statement that represents the original push-down query
     * @param properties user-defined adapter properties
     * @return calculated <code>IMPORT</code> statement
     * @throws AdapterException if the push-down query cannot be generated
     * @throws SQLException     if the metadata for creating the import column
     *                          description cannot be retrieved from the remote data
     *                          source
     */
    public String createImportQuery(final SqlStatement statement, final AdapterProperties properties)
            throws AdapterException, SQLException {
        final ConnectionInformation connectionInformation = getConnectionInformation(this.exaMetadata, properties);
        final String query = createPushdownQuery(statement, properties, this.dialect);
        final String columnDescription = createImportColumnsDescription(this.dialect, query, properties);
        final String importFromPushdownQuery = this.dialect.generatePushdownSql(connectionInformation,
                columnDescription, query);
        LOGGER.finer(() -> "Import from push-down query:\n" + importFromPushdownQuery);
        return importFromPushdownQuery;
    }

    private String createPushdownQuery(final SqlStatement statement, final AdapterProperties properties,
            final SqlDialect dialect) throws AdapterException {
        final SqlGenerationContext context = new SqlGenerationContext(properties.getCatalogName(),
                properties.getSchemaName(), properties.isLocalSource());
        final SqlGenerationVisitor sqlGeneratorVisitor = dialect.getSqlGenerationVisitor(context);
        final String pushdownQuery = statement.accept(sqlGeneratorVisitor);
        LOGGER.finer(() -> "Push-down query:\n" + pushdownQuery);
        return pushdownQuery;
    }

    private String createImportColumnsDescription(final SqlDialect dialect, final String pushdownQuery,
            final AdapterProperties properties) throws SQLException {
        final String columnsDescription = isJdbcImport(properties) ? dialect.describeQueryResultColumns(pushdownQuery)
                : "";
        LOGGER.finer(() -> "Import columns " + columnsDescription);
        return columnsDescription;
    }

    private boolean isJdbcImport(final AdapterProperties properties) {
        return !(properties.isLocalSource() || isExasolSource(properties) || isOracleSource(properties));
    }

    private boolean isExasolSource(final AdapterProperties properties) {
        return properties.isEnabled(IMPORT_FROM_EXA_PROPERTY);
    }

    private boolean isOracleSource(final AdapterProperties properties) {
        return properties.isEnabled(IMPORT_FROM_ORA_PROPERTY);
    }

    private ConnectionInformation getConnectionInformation(final ExaMetadata exaMeta,
            final AdapterProperties properties) {
        final String credentials = getCredentialsForPushdownQuery(exaMeta, properties);
        return new ConnectionInformation(credentials, properties.get(EXA_CONNECTION_STRING_PROPERTY),
                properties.get(ORA_CONNECTION_NAME_PROPERTY));
    }

    /**
     * Get the credentials string for a push-down query
     *
     * @param exaMetadata Exasol metadata
     * @param properties  user-defined adapter properties
     * @return credentials part of the push-down query
     */
    public String getCredentialsForPushdownQuery(final ExaMetadata exaMetadata, final AdapterProperties properties) {
        if (isUserSpecifiedConnection(properties)) {
            return properties.getConnectionName();
        } else {
            return extractCredentialsFromProperties(properties);
        }
    }

    private boolean isUserSpecifiedConnection(final AdapterProperties properties) {
        return (properties.containsKey(AdapterProperties.CONNECTION_NAME_PROPERTY)
                && !properties.getConnectionName().isEmpty());
    }

    // private String getCredentialsForJDBCImport(final ExaMetadata exaMetadata,
    // final AdapterProperties properties) {
//        return "'" + properties.getConnectionString() + "' " + getCredentials(exaMetadata, properties);
//    }
//

//    private String getCredentials(final ExaMetadata exaMetadata, final AdapterProperties properties) {
//        if (isUserSpecifiedConnection(properties)) {
//            return extractCredentialsFromExasolMetadataForNamedConnection(exaMetadata, properties.getConnectionName());
//        } else {
//            return extractCredentialsFromProperties(properties);
//        }
//    }
//
//    private String extractCredentialsFromExasolMetadataForNamedConnection(final ExaMetadata exaMetadata,
//            final String connectionName) {
//        try {
//            final String user = exaMetadata.getConnection(connectionName).getUser();
//            final String password = exaMetadata.getConnection(connectionName).getPassword();
//            if ((user != null) || (password != null)) {
//                return "USER '" + user + "' IDENTIFIED BY '" + password + "'";
//            } else {
//                return "";
//            }
//        } catch (final ExaConnectionAccessException exception) {
//            throw new RemoteConnectionException(
//                    "Could not access the connection information of connection \"" + connectionName + "\"", exception);
//        }
//    }

    private String extractCredentialsFromProperties(final AdapterProperties properties) {
        if (properties.containsKey(AdapterProperties.USERNAME_PROPERTY)) {
            return "USER '" + properties.getUsername() + "' IDENTIFIED BY '" + properties.getPassword() + "'";
        } else {
            return "";
        }
    }
}