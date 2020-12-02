package com.exasol.adapter.dialects.bigquery;

import java.math.BigInteger;
import java.sql.*;
import java.util.StringJoiner;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.sql.SqlStatement;

/**
 * This class implements a BigQuery-specific query rewriter.
 */
public class BigQueryQueryRewriter extends ImportIntoQueryRewriter{
    private static final Logger LOGGER = Logger.getLogger(BigQueryQueryRewriter.class.getName());
    private static final double[] TEN_POWERS = { 10d, 100d, 1000d, 10000d, 100000d, 1000000d };
    @SuppressWarnings("squid:S4784") // this pattern is secure
    private static final Pattern DATE_PATTERN = Pattern.compile("(\\d{4})-(\\d{1,2})-(\\d{1,2})");
    @SuppressWarnings("squid:S4784") // this pattern is secure
    private static final Pattern TIME_PATTERN = Pattern.compile("(\\d{1,2}):(\\d{1,2}):(\\d{1,2})(?:\\.(\\d{1,6}))?");

    /**
     * Create a new instance of the {@link BigQueryQueryRewriter}.
     *
     * @param dialect              Big Query dialect
     * @param remoteMetadataReader remote metadata reader
     * @param connectionFactory    factory for the JDBC connection to remote data source
     */
    public BigQueryQueryRewriter(final SqlDialect dialect, final RemoteMetadataReader remoteMetadataReader,
            final ConnectionFactory connectionFactory) {
        super(dialect, remoteMetadataReader, connectionFactory);
    }

    @Override
    public String rewrite(final SqlStatement statement, final ExaMetadata exaMetadata,
            final AdapterProperties properties) throws AdapterException, SQLException {
        final String query = getQueryFromStatement(statement, properties);
        LOGGER.fine(() -> "Query to rewrite: " + query);
        final StringBuilder builder = new StringBuilder();
        try (final ResultSet resultSet = this.connectionFactory.getConnection().createStatement().executeQuery(query)) {
            builder.append("SELECT * FROM VALUES");
            int rowNumber = 0;
            final ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                if (rowNumber > 0) {
                    builder.append(",");
                }
                appendRow(builder, resultSet, metaData);
                ++rowNumber;
            }
            if (rowNumber == 0) {
                appendQueryForEmptyTable(builder, metaData);
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

    private void appendQueryForEmptyTable(final StringBuilder builder, final ResultSetMetaData metaData)
            throws SQLException {
        final int columnCounter = metaData.getColumnCount();
        builder.append("(");
        final StringJoiner joiner = new StringJoiner(", ");
        for (int i = 0; i < columnCounter; i++) {
            joiner.add("1");
        }
        builder.append(joiner.toString());
        builder.append(") WHERE false");
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
            appendBigInt(builder, resultSet, columnName);
            break;
        case Types.DOUBLE:
            appendDouble(builder, resultSet, columnName);
            break;
        case Types.BOOLEAN:
            appendBoolean(builder, resultSet, columnName);
            break;
        case Types.DATE:
            appendDate(builder, resultSet, columnName);
            break;
        case Types.TIMESTAMP:
            appendTimestamp(builder, resultSet, columnName);
            break;
        case Types.VARCHAR:
            appendVarchar(builder, resultSet, columnName);
            break;
        case Types.TIME:
        case Types.VARBINARY:
        case Types.NUMERIC:
        default:
            appendString(builder, resultSet, columnName);
            break;
        }
    }

    private void appendVarchar(final StringBuilder builder, final ResultSet resultSet, final String columnName)
            throws SQLException {
        final String stringLiteral = this.dialect.getStringLiteral(resultSet.getString(columnName));
        builder.append(resultSet.wasNull() ? "CAST (NULL AS VARCHAR(4))" : stringLiteral);
    }

    private void appendBigInt(final StringBuilder builder, final ResultSet resultSet, final String columnName)
            throws SQLException {
        final String string = resultSet.getString(columnName);
        builder.append(resultSet.wasNull() ? "CAST (NULL AS DECIMAL(19,0))" : new BigInteger(string));
    }

    private void appendDouble(final StringBuilder builder, final ResultSet resultSet, final String columnName)
            throws SQLException {
        final double value = resultSet.getDouble(columnName);
        builder.append(resultSet.wasNull() ? "CAST (NULL AS DOUBLE)" : value);
    }

    private void appendBoolean(final StringBuilder builder, final ResultSet resultSet, final String columnName)
            throws SQLException {
        final boolean value = resultSet.getBoolean(columnName);
        builder.append(resultSet.wasNull() ? "CAST (NULL AS BOOLEAN)" : value);
    }

    private void appendDate(final StringBuilder builder, final ResultSet resultSet, final String columnName)
            throws SQLException {
        final String value = resultSet.getString(columnName);
        if (value == null) {
            builder.append("CAST (NULL AS VARCHAR(4))");
        } else {
            builder.append("'");
            builder.append(castDate(value));
            builder.append("'");
        }
    }

    private String castDate(final String dateToCast) {
        final Matcher matcher = DATE_PATTERN.matcher(dateToCast);
        if (matcher.matches()) {
            final int year = Integer.parseInt(matcher.group(1));
            final int month = Integer.parseInt(matcher.group(2));
            final int day = Integer.parseInt(matcher.group(3));
            return String.format("%02d.%02d.%04d", day, month, year);
        } else {
            throw new IllegalArgumentException(
                    "Date does not match required format: YYYY-[M]M-[D]D. Actual value was:" + dateToCast);
        }
    }

    private void appendTimestamp(final StringBuilder builder, final ResultSet resultSet, final String columnName)
            throws SQLException {
        final String value = resultSet.getString(columnName);
        if (value == null) {
            builder.append("CAST (NULL AS VARCHAR(4))");
        } else {
            builder.append("'");
            builder.append(castTimestamp(value));
            builder.append("'");
        }
    }

    private String castTimestamp(final String timestampToCast) {
        final StringBuilder builder = new StringBuilder();
        final String[] splitTimestamp = getSplitTimestamp(timestampToCast);
        builder.append(castDate(splitTimestamp[0]));
        builder.append(" ");
        builder.append(castTime(splitTimestamp[1]));
        return builder.toString();
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
                        .round((fractionOfSecondInt / TEN_POWERS[fractionOfSecond.length() - 1]) * 1000);
                return String.format("%02d:%02d:%02d.%03d", hour, minute, second, fractionOfSecondRounded);
            } else {
                return String.format("%02d:%02d:%02d", hour, minute, second);
            }
        } else {
            throw new IllegalArgumentException("Time does not match required format: [H]H:[M]M:[S]S[.DDDDDD]]");
        }
    }

    private void appendString(final StringBuilder builder, final ResultSet resultSet, final String columnName)
            throws SQLException {
        final String value = resultSet.getString(columnName);
        if (value == null) {
            builder.append("CAST (NULL AS VARCHAR(4))");
        } else {
            builder.append(this.dialect.getStringLiteral(value));
        }
    }
}