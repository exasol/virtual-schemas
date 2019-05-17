package com.exasol.adapter.jdbc;

import java.sql.SQLException;
import java.util.logging.Logger;

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
        final String columnsDescription = isJdbcImport() ? this.dialect.describeQueryResultColumns(pushdownQuery) : "";
        LOGGER.finer(() -> "Import columns " + columnsDescription);
        return columnsDescription;
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

    private ConnectionInformation getConnectionInformation() {
        final String credentials = getConnectionDefinition();
        return new ConnectionInformation(credentials, this.properties.get(EXA_CONNECTION_STRING_PROPERTY),
                this.properties.get(ORA_CONNECTION_NAME_PROPERTY));
    }

    /**
     * Get the connection definition part of a push-down query
     *
     * @return credentials part of the push-down query
     */
    public String getConnectionDefinition() {
        final StringBuilder builder = new StringBuilder();
        if (isUserSpecifiedConnection()) {
            appendConnection(builder);
        } else {
            appendUserPasswordBasedConnectionString(builder);
        }
        return builder.toString();
    }

    private boolean isUserSpecifiedConnection() {
        return (this.properties.containsKey(AdapterProperties.CONNECTION_NAME_PROPERTY)
                && !this.properties.getConnectionName().isEmpty());
    }

    private void appendConnection(final StringBuilder builder) {
        builder.append(this.properties.getConnectionName());
        if (this.properties.containsKey(AdapterProperties.USERNAME_PROPERTY)) {
            if (this.properties.containsKey(AdapterProperties.PASSWORD_PROPERTY)) {
                builder.append(" ");
                appendCredentialsFromProperties(builder);
            } else {
                throw new IllegalArgumentException(createOverrideErrorMessage("password"));
            }
        } else {
            if (this.properties.containsKey(AdapterProperties.PASSWORD_PROPERTY)) {
                throw new IllegalArgumentException(createOverrideErrorMessage("username"));
            }
        }
    }

    private void appendUserPasswordBasedConnectionString(final StringBuilder builder) {
        if (this.properties.containsKey(AdapterProperties.CONNECTION_STRING_PROPERTY)) {
            builder.append("'");
            builder.append(this.properties.getConnectionString());
            builder.append("' ");
        }
        appendCredentialsFromProperties(builder);
    }

    private void appendCredentialsFromProperties(final StringBuilder builder) {
        if (this.properties.containsKey(AdapterProperties.USERNAME_PROPERTY)) {
            builder.append("USER '" + this.properties.getUsername() + "' IDENTIFIED BY '"
                    + this.properties.getPassword() + "'");
        }
    }

    private String createOverrideErrorMessage(final String credential) {
        return "The " + credential + " is missing when trying to override credentials from connection \""
                + this.properties.getConnectionName() + "\". Specify " + AdapterProperties.USERNAME_PROPERTY + " and "
                + AdapterProperties.PASSWORD_PROPERTY + " to override connection credentials";
    }
}