package com.exasol.adapter.dialects.bigquery;

import java.sql.*;
import java.util.logging.*;

import com.exasol.*;
import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.*;

/**
 * This class implements a BigQuery-specific query rewriter
 */
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
            int rowNumber = 0;
            while (resultSet.next()) {
                if (rowNumber > 0) {
                    builder.append(",");
                }
                appendRow(builder, resultSet, resultSet.getMetaData());
                ++rowNumber;
            }
            if (rowNumber == 0) {
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
            final ResultSetMetaData resultSetMetadata) throws SQLException {
        final int columnCount = resultSetMetadata.getColumnCount();
        builder.append(" (");
        for (int i = 1; i <= columnCount; ++i) {
            final String columnName = resultSetMetadata.getColumnName(i);
            if (i > 1) {
                builder.append(", ");
            }
            appendColumnValue(builder, resultSet, columnName, resultSetMetadata.getColumnType(i));
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
            builder.append("'");
            builder.append(castDate(resultSet.getString(columnName)));
            builder.append("'");
            break;
        case Types.TIMESTAMP:
            builder.append("'");
            builder.append(castTimestamp(resultSet.getString(columnName)));
            builder.append("'");
            break;
        case Types.VARBINARY:
        case Types.TIME:
        case Types.VARCHAR:
        default:
            builder.append("'");
            builder.append(resultSet.getString(columnName));
            builder.append("'");
            break;
        }
    }

    private String castTimestamp(final String timestampToCast) {
        final String[] splitTimestamp = getSplitTimestamp(timestampToCast);
        final StringBuilder builder = new StringBuilder();
        builder.append(castDate(splitTimestamp[0]));
        builder.append(" ");
        builder.append(getTime(splitTimestamp[1]));
        return builder.toString();
    }

    private String[] getSplitTimestamp(final String timestampToCast) {
        if (timestampToCast.contains("T")) {
            return timestampToCast.split("T");
        } else {
            return timestampToCast.split(" ");
        }
    }

    private String getTime(final String time) {
        final String[] splitTime = time.split("\\.");
        final String[] timeWithoutSeconds = splitTime[0].split(":");
        final StringBuilder builder = new StringBuilder();
        final int timeWithoutSecondsLength = timeWithoutSeconds.length;
        for (int i = 0; i <= timeWithoutSecondsLength - 1; ++i) {
            if (i <= timeWithoutSecondsLength - 1 && timeWithoutSeconds[i].length() != 2) {
                builder.append("0");
            }
            builder.append(timeWithoutSeconds[i]);
            if (i < timeWithoutSecondsLength - 1) {
                builder.append(":");
            }
        }
        if (splitTime.length > 1) {
            builder.append(".");
            builder.append(splitTime[1], 0, 3);
        }
        return builder.toString();
    }

    private String castDate(final String dateToCast) {
        final String[] dates = dateToCast.split("-");
        final StringBuilder builder = new StringBuilder();
        for (int i = dates.length - 1; i >= 0; --i) {
            if (i > 0 && dates[i].length() != 2) {
                builder.append("0");
            }
            builder.append(dates[i]);
            if (i > 0) {
                builder.append(".");
            }
        }
        return builder.toString();
    }
}