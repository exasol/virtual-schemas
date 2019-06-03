package com.exasol.adapter.dialects.bigquery;

import java.sql.*;
import java.util.logging.*;
import java.util.regex.*;

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
    private static final double[] TEN_POWERS = { 10d, 100d, 1000d, 10000d, 100000d, 1000000d };
    private static final Pattern DATE_PATTERN = Pattern.compile("(\\d{4})-(\\d{1,2})-(\\d{1,2})");
    private static final Pattern TIME_PATTERN = Pattern.compile("(\\d{1,2}):(\\d{1,2}):(\\d{1,2})(?:\\.(\\d{1,6}))?");

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
                final ResultSetMetaData metaData = resultSet.getMetaData();
                appendRow(builder, resultSet, metaData);
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
        if (timestampToCast != null) {
            final StringBuilder builder = new StringBuilder();
            final String[] splitTimestamp = getSplitTimestamp(timestampToCast);
            builder.append(castDate(splitTimestamp[0]));
            builder.append(" ");
            builder.append(castTime(splitTimestamp[1]));
            return builder.toString();
        }
        return null;
    }

    private String[] getSplitTimestamp(final String timestampToCast) {
        if (timestampToCast.contains("T")) {
            return timestampToCast.split("T");
        } else {
            return timestampToCast.split(" ");
        }
    }

    private String castTime(final String timeToCast) {
        final Matcher matcher = TIME_PATTERN.matcher(timeToCast);
        if (matcher.matches()) {
            final int hour = Integer.parseInt(matcher.group(1));
            final int minute = Integer.parseInt(matcher.group(2));
            final int second = Integer.parseInt(matcher.group(3));
            final String fractionOfSecond = matcher.group(4);
            if (fractionOfSecond != null) {
                final int fractionOfSecondInt = Integer.parseInt(fractionOfSecond);
                final int fractionOfSecondRounded = (int) Math
                        .round(fractionOfSecondInt / TEN_POWERS[fractionOfSecond.length() - 1] * 1000);
                return String.format("%02d:%02d:%02d.%03d", hour, minute, second, fractionOfSecondRounded);
            } else {
                return String.format("%02d:%02d:%02d", hour, minute, second);
            }
        } else {
            throw new IllegalArgumentException("Time does not match required format: [H]H:[M]M:[S]S[.DDDDDD]]");
        }
    }

    private String castDate(final String dateToCast) {
        if (dateToCast != null) {
            final Matcher matcher = DATE_PATTERN.matcher(dateToCast);
            if (matcher.matches()) {
                final int year = Integer.parseInt(matcher.group(1));
                final int month = Integer.parseInt(matcher.group(2));
                final int day = Integer.parseInt(matcher.group(3));
                return String.format("%02d.%02d.%04d", day, month, year);
            } else {
                throw new IllegalArgumentException("Date does not match required format: YYYY-[M]M-[D]D");
            }
        } else {
            return null;
        }
    }
}