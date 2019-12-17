package com.exasol.adapter.dialects.exasol;

import static com.exasol.adapter.dialects.exasol.ExasolProperties.EXASOL_IMPORT_PROPERTY;

import java.sql.Connection;
import java.sql.SQLException;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionDefinitionBuilder;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.sql.SqlNodeVisitor;
import com.exasol.adapter.sql.SqlStatement;

/**
 * Exasol-specific query rewriter.
 * <p>
 * This query rewriter supports Exasol specific parts of the <code>IMPORT</code> statement:
 * <code>IMPORT ... FROM EXA</code> and 1:1 push-down query take-over.
 * </p>
 */
public class ExasolQueryRewriter extends BaseQueryRewriter {
    /**
     * Create a new instance of the {@link ExasolQueryRewriter}.
     *
     * @param dialect              dialect
     * @param remoteMetadataReader remote metadata reader
     * @param connection           JDBC connection to remote data source
     */
    public ExasolQueryRewriter(final SqlDialect dialect, final RemoteMetadataReader remoteMetadataReader,
            final Connection connection) {
        super(dialect, remoteMetadataReader, connection);
    }

    @Override
    protected ConnectionDefinitionBuilder createConnectionDefinitionBuilder() {
        return new ExasolConnectionDefinitionBuilder();
    }

    @Override
    public String rewrite(final SqlStatement statement, final ExaMetadata exaMetadata,
            final AdapterProperties properties) throws AdapterException, SQLException {
        if (properties.isLocalSource()) {
            return convertOriginalQueryToString(statement, properties);
        } else if (isImportFromExa(properties)) {
            return rewriteToImportFromExa(statement, exaMetadata, properties);
        } else {
            return super.rewrite(statement, exaMetadata, properties);
        }
    }

    private String convertOriginalQueryToString(final SqlStatement statement, final AdapterProperties properties)
            throws AdapterException {
        final SqlGenerationContext context = new SqlGenerationContext(properties.getCatalogName(),
                properties.getSchemaName(), false);
        final SqlNodeVisitor<String> sqlGeneratorVisitor = this.dialect.getSqlGenerationVisitor(context);
        return statement.accept(sqlGeneratorVisitor);
    }

    private boolean isImportFromExa(final AdapterProperties properties) {
        return properties.isEnabled(EXASOL_IMPORT_PROPERTY);
    }

    private String rewriteToImportFromExa(final SqlStatement statement, final ExaMetadata exaMetadata,
            final AdapterProperties properties) throws AdapterException {
        final SqlGenerationContext context = new SqlGenerationContext(properties.getCatalogName(),
                properties.getSchemaName(), false);
        final SqlNodeVisitor<String> sqlGeneratorVisitor = this.dialect.getSqlGenerationVisitor(context);
        final ExaConnectionInformation exaConnectionInformation = getConnectionInformation(exaMetadata, properties);
        final String connectionDefinition = this.connectionDefinitionBuilder.buildConnectionDefinition(properties,
                exaConnectionInformation);
        return "IMPORT FROM EXA " + connectionDefinition + " STATEMENT '"
                + statement.accept(sqlGeneratorVisitor).replace("'", "''") + "'";
    }
}