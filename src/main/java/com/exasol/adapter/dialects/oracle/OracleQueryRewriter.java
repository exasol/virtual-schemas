package com.exasol.adapter.dialects.oracle;

import java.sql.SQLException;

import com.exasol.adapter.dialects.AbstractQueryRewriter;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.jdbc.*;

/**
 * This class implements an Oracle-specific query rewriter.
 */
public class OracleQueryRewriter extends AbstractQueryRewriter {
    /**
     * Create a new instance of the {@link OracleQueryRewriter}.
     *
     * @param dialect              Oracle SQl dialect
     * @param remoteMetadataReader reader for metadata from the remote data source
     */
    public OracleQueryRewriter(final SqlDialect dialect, final RemoteMetadataReader remoteMetadataReader,
            final ConnectionFactory connectionFactory) {
        super(dialect, remoteMetadataReader);
    }

    @Override
    protected ConnectionDefinitionBuilder createConnectionDefinitionBuilder() {
        return new OracleConnectionDefinitionBuilder();
    }

    @Override
    protected String generateImportStatement(final String connectionDefinition, final String pushdownQuery)
            throws SQLException {
        return "IMPORT FROM ORA " + connectionDefinition + " STATEMENT '" + pushdownQuery.replace("'", "''") + "'";
    }
}