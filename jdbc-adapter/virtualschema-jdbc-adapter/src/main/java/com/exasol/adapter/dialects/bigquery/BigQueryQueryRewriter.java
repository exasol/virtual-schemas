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
        final String query = getQueryFromStatement(statement, properties);
        LOGGER.fine(() -> "Query to rewrite: " + query);
        try (final ResultSet resultSet = this.connection.createStatement().executeQuery(query)) {
            builder.append("SELECT * FROM VALUES");
            boolean first = true;
            int rowEmpty = 0;
            while (resultSet.next()) {
                rowEmpty++;
                if (!first) {
                    builder.append(",");
                } else {
                    first = false;
                }
                appendRow(builder, resultSet, resultSet.getMetaData());
            }
            if (rowEmpty == 0) {
                builder.append(" (1) WHERE false");
                return builder.toString();
            }
        }
        return builder.toString();
    }

    private String getQueryFromStatement(final SqlStatement statement, final AdapterProperties properties)
            throws AdapterException {
        final SqlGenerationContext context = new SqlGenerationContext(properties.getCatalogName(),
                properties.getSchemaName(), false);
        return statement.accept(this.dialect.getSqlGenerationVisitor(context));
    }

    private void appendRow(final StringBuilder builder, final ResultSet resultSet,
            final ResultSetMetaData resultSetNetadata) throws SQLException {
        final int columnCount = resultSetNetadata.getColumnCount();
        LOGGER.info("Column count: " + resultSetNetadata.getColumnCount());
        builder.append(" (");
        for (int i = 1; i <= columnCount; i++) {
            final String columnName = resultSetNetadata.getColumnName(i);
            appendColumnValue(builder, resultSet, columnName, resultSetNetadata.getColumnType(i));
            if (i < columnCount) {
                builder.append(", ");
            }
        }
        builder.append(")");
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
