package com.exasol.adapter.dialects.bigquery;

import com.exasol.*;
import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.*;

import java.sql.*;

public class BigQueryQueryRewriter extends BaseQueryRewriter {
    /**
     * Create a new instance of a {@link BigQueryQueryRewriter}
     *
     * @param dialect              dialect
     * @param remoteMetadataReader remote metadata reader
     * @param connection           JDBC connection to remote data source
     */
    public BigQueryQueryRewriter(final SqlDialect dialect, final RemoteMetadataReader remoteMetadataReader,
            final Connection connection) {
        super(dialect, remoteMetadataReader, connection);
    }

    @Override
    public String rewrite(final SqlStatement statement, final ExaMetadata exaMetadata,
            final AdapterProperties properties) throws AdapterException, SQLException {
        final SqlGenerationContext context = new SqlGenerationContext(properties.getCatalogName(),
                properties.getSchemaName(), false);
        final SqlGenerationVisitor visitor = this.dialect.getSqlGenerationVisitor(context);
        final String query = statement.accept(visitor);
        final StringBuilder builder = new StringBuilder();

        try (final Statement jdbcStatement = this.connection.createStatement()) {
            System.out.println(query);
            try (final ResultSet resultSet = jdbcStatement.executeQuery(query)) {
                final ResultSetMetaData metadata = resultSet.getMetaData();
                final int columnCount = metadata.getColumnCount();
                builder.append("SELECT * FROM (VALUES ");
                while (resultSet.next()) {
                    builder.append("(");
                    for (int i = 1; i <= columnCount; i++) {
                        builder.append(metadata.getColumnName(i));

                        if (!resultSet.last()) {
                            builder.append(", ");
                        }
                    }
                    builder.append(")");
                }
            }
        }
        builder.append(")");
        return builder.toString();
    }
}
