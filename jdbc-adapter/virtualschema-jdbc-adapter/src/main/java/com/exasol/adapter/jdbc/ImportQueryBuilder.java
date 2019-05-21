package com.exasol.adapter.jdbc;

import java.sql.SQLException;
import java.util.logging.Logger;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.sql.SqlStatement;

/**
 * This class creates <code>IMPORT</code> statements from push-down queries.
 *
 * @see <a href="https://docs.exasol.com/sql/import.htm">IMPORT (Exasol
 *      documentation)</a>
 */
public class ImportQueryBuilder {
    private static final Logger LOGGER = Logger.getLogger(ImportQueryBuilder.class.getName());
    private static final String IMPORT_FROM_EXA_PROPERTY = "IMPORT_FROM_EXA";
    private static final String IMPORT_FROM_ORA_PROPERTY = "IMPORT_FROM_ORA";
    private static final String EXA_CONNECTION_STRING_PROPERTY = "EXA_CONNECTION_STRING";
    private static final String ORA_CONNECTION_NAME_PROPERTY = "ORA_CONNECTION_NAME";
    private SqlDialect dialect;
    private SqlStatement statement;
    private AdapterProperties properties;
    private ExaMetadata exaMetadata;

    /**
     * Set the SQL dialect for which the IMPORT query is built
     *
     * @param dialect SQL dialect
     * @return <code>this</code> for fluent programming
     */
    public ImportQueryBuilder dialect(final SqlDialect dialect) {
        this.dialect = dialect;
        return this;
    }

    /**
     * Set the original push-down statement
     *
     * @param statement SQL statement that represents the original push-down query
     * @return <code>this</code> for fluent programming
     */
    public ImportQueryBuilder statement(final SqlStatement statement) {
        this.statement = statement;
        return this;
    }

    /**
     * Set the properties
     *
     * @param properties user-defined adapter properties
     * @return <code>this</code> for fluent programming
     */
    public ImportQueryBuilder properties(final AdapterProperties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Set the Exasol metadata
     *
     * @param exaMetadata Exasol metadata
     * @return <code>this</code> for fluent programming
     */
    public ImportQueryBuilder exaMetadata(final ExaMetadata exaMetadata) {
        this.exaMetadata = exaMetadata;
        return this;
    }

    /**
     * Create the <code>IMPORT</code> statement
     *
     * @return calculated <code>IMPORT</code> statement
     * @throws AdapterException if the push-down query cannot be generated
     * @throws SQLException     if the metadata for creating the import column
     *                          description cannot be retrieved from the remote data
     *                          source
     */
    public String build() throws AdapterException, SQLException {
        final ConnectionInformation connectionInformation = getConnectionInformation();
        final String query = createPushdownQuery();
        final String columnDescription = createImportColumnsDescription(query);
        final String importFromPushdownQuery = this.dialect.generatePushdownSql(connectionInformation,
                columnDescription, query);
        LOGGER.finer(() -> "Import from push-down query:\n" + importFromPushdownQuery);
        return importFromPushdownQuery;
    }

    private String createPushdownQuery() throws AdapterException {
        final SqlGenerationContext context = new SqlGenerationContext(this.properties.getCatalogName(),
                this.properties.getSchemaName(), this.properties.isLocalSource());
        final SqlGenerationVisitor sqlGeneratorVisitor = this.dialect.getSqlGenerationVisitor(context);
        final String pushdownQuery = this.statement.accept(sqlGeneratorVisitor);
        LOGGER.finer(() -> "Push-down query:\n" + pushdownQuery);
        return pushdownQuery;
    }

    private String createImportColumnsDescription(final String pushdownQuery) throws SQLException {
        if (isJdbcImport()) {
            final String columnsDescription = this.dialect.describeQueryResultColumns(pushdownQuery);
            LOGGER.finer(() -> "Import columns: " + columnsDescription);
            return columnsDescription;
        } else {
            return null;
        }
    }

    private boolean isJdbcImport() {
        return !(this.properties.isLocalSource() || isExasolSource() || isOracleSource());
    }

    private boolean isExasolSource() {
        return this.properties.isEnabled(IMPORT_FROM_EXA_PROPERTY);
    }

    private boolean isOracleSource() {
        return this.properties.isEnabled(IMPORT_FROM_ORA_PROPERTY);
    }

    private ConnectionInformation getConnectionInformation() throws AdapterException {
        final String credentials = getConnectionDefinition();
        return new ConnectionInformation(credentials, this.properties.get(EXA_CONNECTION_STRING_PROPERTY),
                this.properties.get(ORA_CONNECTION_NAME_PROPERTY));
    }

    /**
     * Get the connection definition part of a push-down query
     *
     * @return credentials part of the push-down query
     * @throws AdapterException if Exasol metadata is unaccessible
     */
    public String getConnectionDefinition() throws AdapterException {
        if (this.properties.hasConnectionString() && this.properties.hasUsername() && this.properties.hasPassword()) {
            return getConnectionFromPropertiesOnly();
        } else if (this.properties.hasConnectionName()) {
            return getConnectionWithExaMetadata();
        } else {
            throw new IllegalArgumentException(
                    "Incomplete remote connection information. Please specify at least a named connection with "
                            + AdapterProperties.CONNECTION_NAME_PROPERTY + " or individual connetion properties "
                            + AdapterProperties.CONNECTION_STRING_PROPERTY + ", " + AdapterProperties.USERNAME_PROPERTY
                            + " and " + AdapterProperties.PASSWORD_PROPERTY + ".");
        }
    }

    private String getConnectionFromPropertiesOnly() {
        return getConnectionDefinition(this.properties.getConnectionString(), this.properties.getUsername(),
                this.properties.getPassword());
    }

    private String getConnectionWithExaMetadata() throws AdapterException {
        ExaConnectionInformation exaConnection;
        try {
            exaConnection = this.exaMetadata.getConnection(this.properties.getConnectionName());
            final String connectionString = this.properties.hasConnectionString()
                    ? this.properties.getConnectionString()
                    : exaConnection.getAddress();
            final String username = this.properties.hasUsername() ? this.properties.getUsername()
                    : exaConnection.getUser();
            final String password = this.properties.hasPassword() ? this.properties.getPassword()
                    : exaConnection.getPassword();
            return getConnectionDefinition(connectionString, username, password);
        } catch (final ExaConnectionAccessException exception) {
            throw new AdapterException("Unable to retrieve Exasol metadata trying to construct connection definition.",
                    exception);
        }
    }

    private String getConnectionDefinition(final String connectionString, final String username,
            final String password) {
        final StringBuilder builder = new StringBuilder();
        builder.append("'");
        builder.append(connectionString);
        builder.append("' USER '");
        builder.append(username);
        builder.append("' IDENTIFIED BY '");
        builder.append(password);
        builder.append("'");
        return builder.toString();
    }
}