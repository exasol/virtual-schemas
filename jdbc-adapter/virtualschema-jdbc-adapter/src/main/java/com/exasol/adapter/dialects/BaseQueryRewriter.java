package com.exasol.adapter.dialects;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import com.exasol.*;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.SqlNodeVisitor;
import com.exasol.adapter.sql.SqlStatement;

/**
 * Base implementation of {@link QueryRewriter}.
 */
public class BaseQueryRewriter implements QueryRewriter {
    private static final Logger LOGGER = Logger.getLogger(BaseQueryRewriter.class.getName());
    protected final SqlDialect dialect;
    protected final RemoteMetadataReader remoteMetadataReader;
    protected final Connection connection;
    protected final ConnectionDefinitionBuilder connectionDefinitionBuilder;

    /**
     * Create a new instance of a {@link BaseQueryRewriter}.
     *
     * @param dialect              dialect
     * @param remoteMetadataReader remote metadata reader
     * @param connection           JDBC connection to remote data source
     */
    public BaseQueryRewriter(final SqlDialect dialect, final RemoteMetadataReader remoteMetadataReader,
            final Connection connection) {
        this.dialect = dialect;
        this.remoteMetadataReader = remoteMetadataReader;
        this.connection = connection;
        this.connectionDefinitionBuilder = createConnectionDefinitionBuilder();
    }

    /**
     * Create the connection definition builder.
     * <p>
     * Override this method in case you need connection definitions that differ from the regular JDBC style.
     *
     * @return connection definition builder
     */
    protected ConnectionDefinitionBuilder createConnectionDefinitionBuilder() {
        return new BaseConnectionDefinitionBuilder();
    }

    @Override
    public String rewrite(final SqlStatement statement, final ExaMetadata exaMetadata,
            final AdapterProperties properties) throws AdapterException, SQLException {
        final String query = createPushdownQuery(statement, properties);
        final String columnDescription = createImportColumnsDescription(query);
        final ExaConnectionInformation exaConnectionInformation = getConnectionInformation(exaMetadata, properties);
        final String connectionDefinition = this.connectionDefinitionBuilder.buildConnectionDefinition(properties,
                exaConnectionInformation);
        final String importFromPushdownQuery = generatePushdownSql(columnDescription, connectionDefinition, query);
        LOGGER.finer(() -> "Import from push-down query:\n" + importFromPushdownQuery);
        return importFromPushdownQuery;
    }

    private String createPushdownQuery(final SqlStatement statement, final AdapterProperties properties)
            throws AdapterException {
        final SqlGenerationContext context = new SqlGenerationContext(properties.getCatalogName(),
                properties.getSchemaName(), false);
        final SqlNodeVisitor<String> sqlGeneratorVisitor = this.dialect.getSqlGenerationVisitor(context);
        final String pushdownQuery = statement.accept(sqlGeneratorVisitor);
        LOGGER.finer(() -> "Push-down query generated with " + sqlGeneratorVisitor.getClass().getSimpleName() + ":\n"
                + pushdownQuery);
        return pushdownQuery;
    }

    private String createImportColumnsDescription(final String query) {
        final ColumnMetadataReader columnMetadataReader = this.remoteMetadataReader.getColumnMetadataReader();
        final ResultSetMetadataReader resultSetMetadataReader = new ResultSetMetadataReader(this.connection,
                columnMetadataReader);
        final String columnsDescription = resultSetMetadataReader.describeColumns(query);
        LOGGER.finer(() -> "Import columns: " + columnsDescription);
        return columnsDescription;
    }

    protected ExaConnectionInformation getConnectionInformation(final ExaMetadata exaMetadata,
            final AdapterProperties properties) throws AdapterException {
        final ExaConnectionInformation exaConnectionInformation;
        if (properties.hasConnectionName()) {
            final String connectionName = properties.getConnectionName();
            try {
                exaConnectionInformation = exaMetadata.getConnection(connectionName);
            } catch (final ExaConnectionAccessException exception) {
                throw new AdapterException("Unable to access information about the Exasol connection named \""
                        + connectionName + "\" trying to create a connection definition for rewritten query.",
                        exception);
            }
        } else {
            exaConnectionInformation = null;
        }
        return exaConnectionInformation;
    }

    private String generatePushdownSql(final String columnDescription, final String connectionDefinition,
            final String pushdownSql) {
        return "IMPORT INTO (" + columnDescription + ") FROM JDBC " + connectionDefinition + " STATEMENT '"
                + pushdownSql.replace("'", "''") + "'";
    }
}