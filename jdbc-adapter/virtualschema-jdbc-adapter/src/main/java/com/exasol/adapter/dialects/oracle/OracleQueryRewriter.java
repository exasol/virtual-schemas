package com.exasol.adapter.dialects.oracle;

import java.sql.Connection;
import java.sql.SQLException;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionDefinitionBuilder;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.sql.SqlStatement;

/**
 * This class implements an Oracle-specific query rewriter.
 */
public class OracleQueryRewriter extends BaseQueryRewriter {
    /**
     * Create a new instance of the {@link OracleQueryRewriter}.
     *
     * @param dialect              Oracle SQl dialect
     * @param remoteMetadataReader reader for metadata from the remote data source
     * @param connection           JDBC connection to the remote data source
     */
    public OracleQueryRewriter(final SqlDialect dialect, final RemoteMetadataReader remoteMetadataReader,
            final Connection connection) {
        super(dialect, remoteMetadataReader, connection);
    }

    @Override
    protected ConnectionDefinitionBuilder createConnectionDefinitionBuilder() {
        return new OracleConnectionDefinitionBuilder();
    }

    @Override
    public String rewrite(final SqlStatement statement, final ExaMetadata exaMetadata,
            final AdapterProperties properties) throws AdapterException, SQLException {
        if (isImportFromOra(properties)) {
            return rewriteToImportFromOra(statement, exaMetadata, properties);
        } else {
            return super.rewrite(statement, exaMetadata, properties);
        }
    }

    private boolean isImportFromOra(final AdapterProperties properties) {
        return properties.isEnabled(OracleProperties.ORACLE_IMPORT_PROPERTY);
    }

    private String rewriteToImportFromOra(final SqlStatement statement, final ExaMetadata exaMetadata,
            final AdapterProperties properties) throws AdapterException {
        final SqlGenerationContext context = new SqlGenerationContext(properties.getCatalogName(),
                properties.getSchemaName(), false);
        final SqlGenerationVisitor sqlGeneratorVisitor = this.dialect.getSqlGenerationVisitor(context);
        final ExaConnectionInformation exaConnectionInformation = getConnectionInformation(exaMetadata, properties);
        final String connectionDefinition = this.connectionDefinitionBuilder.buildConnectionDefinition(properties,
                exaConnectionInformation);
        return "IMPORT FROM ORA " + connectionDefinition + " STATEMENT '"
                + statement.accept(sqlGeneratorVisitor).replace("'", "''") + "'";
    }
}