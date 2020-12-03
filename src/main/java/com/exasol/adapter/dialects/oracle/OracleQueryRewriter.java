package com.exasol.adapter.dialects.oracle;

import java.sql.SQLException;

import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.SqlNodeVisitor;
import com.exasol.adapter.sql.SqlStatement;

/**
 * This class implements an Oracle-specific query rewriter.
 */
public class OracleQueryRewriter extends ImportIntoQueryRewriter {
    /**
     * Create a new instance of the {@link OracleQueryRewriter}.
     *
     * @param dialect              Oracle SQl dialect
     * @param remoteMetadataReader reader for metadata from the remote data source
     * @param connectionFactory    factory for the JDBC connection to the remote data source
     */
    public OracleQueryRewriter(final SqlDialect dialect, final RemoteMetadataReader remoteMetadataReader,
            final ConnectionFactory connectionFactory) {
        super(dialect, remoteMetadataReader, connectionFactory);
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
        final SqlNodeVisitor<String> sqlGeneratorVisitor = this.dialect.getSqlGenerationVisitor(context);
        final ExaConnectionInformation exaConnectionInformation = getConnectionInformation(exaMetadata, properties);
        final String connectionDefinition = this.connectionDefinitionBuilder.buildConnectionDefinition(properties,
                exaConnectionInformation);
        return "IMPORT FROM ORA " + connectionDefinition + " STATEMENT '"
                + statement.accept(sqlGeneratorVisitor).replace("'", "''") + "'";
    }
}