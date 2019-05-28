package com.exasol.adapter.dialects.bigquery;

import java.sql.*;
import java.util.logging.*;

import com.exasol.*;
import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.*;

public class BigQueryQueryRewriter extends BaseQueryRewriter {
    private static final Logger LOGGER = Logger.getLogger(BigQueryQueryRewriter.class.getName());

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
        final StringBuilder builder = new StringBuilder();

        try (final Statement jdbcStatement = this.connection.createStatement()) {
            final SqlGenerationContext context = new SqlGenerationContext(properties.getCatalogName(),
                    properties.getSchemaName(), false);
            final SqlGenerationVisitor visitor = this.dialect.getSqlGenerationVisitor(context);
            final String query = statement.accept(visitor);
            LOGGER.info("Query: " + query);
            try (final ResultSet resultSet = jdbcStatement.executeQuery(query)) {
                builder.append("SELECT * FROM VALUES");
                final ResultSetMetaData resultSetNetadata = resultSet.getMetaData();
                int rowEmpty = 0;
                boolean first = true;
                while (resultSet.next()) {
                    rowEmpty++;
                    if (!first) {
                        builder.append(",");
                    } else {
                        first = false;
                    }

                    final int columnCount = resultSetNetadata.getColumnCount();
                    LOGGER.info("Column count: " + resultSetNetadata.getColumnCount());
                    builder.append(" (");
                    for (int i = 1; i <= columnCount; i++) {
                        final int type = resultSetNetadata.getColumnType(i);
                        LOGGER.info("Column type: " + type);
                        LOGGER.info("Column name: " + resultSetNetadata.getColumnName(i));
                        final String columnName = resultSetNetadata.getColumnName(i);
                        this.appendColumnValue(builder, resultSet, columnName, type);
                        if (i < columnCount) {
                            builder.append(", ");
                        }
                    }
                    builder.append(")");
                }
                if (rowEmpty == 0) {
                    builder.append(" (1) WHERE false");
                    return builder.toString();
                }
            }
        }
        return builder.toString();
    }

    private void appendColumnValue(final StringBuilder builder, final ResultSet resultSet, final String columnName,
            final int type) throws SQLException {
        switch (type) {
        case Types.BIGINT:
            builder.append(resultSet.getInt(columnName));
            break;
        case Types.NUMERIC:
        case Types.DOUBLE:
            builder.append(resultSet.getDouble(columnName));
            break;
        case Types.BOOLEAN:
            builder.append(resultSet.getBoolean(columnName));
            break;
        case Types.DATE:
        case Types.TIMESTAMP:
        case Types.TIME:
        case Types.VARCHAR:
        case Types.VARBINARY:
        default:
            builder.append("'");
            builder.append(resultSet.getString(columnName));
            builder.append("'");
            break;
        }
    }
}
