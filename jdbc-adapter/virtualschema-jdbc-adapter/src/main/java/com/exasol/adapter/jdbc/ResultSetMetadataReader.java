package com.exasol.adapter.jdbc;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

/**
 * This class creates a textual description of the result columns of a push-down query.
 * <p>
 * The columns description is necessary to prepare the <code>IMPORT</code> statement in which the push-down query
 * is executed.
 */
public class ResultSetMetadataReader {
    private static final Logger LOGGER = Logger.getLogger(ResultSetMetadataReader.class.getName());
    private final Connection connection;
    private final ColumnMetadataReader columnMetadataReader;

    /**
     * Create a new instance of a {@link ResultSetMetadataReader}.
     *
     * @param connection           connection to the remote data source
     * @param columnMetadataReader column metadata reader used to translate the column types
     */
    public ResultSetMetadataReader(final Connection connection, final ColumnMetadataReader columnMetadataReader) {
        this.connection = connection;
        this.columnMetadataReader = columnMetadataReader;
    }

    /**
     * Generate a textual description of the result columns of the push-down query.
     *
     * @param query push-down query
     * @return string describing the columns (names and types)
     * @throws SQLException if the necessary remote metadata cannot be read
     */
    public String describeColumns(final String query) throws SQLException {
        LOGGER.fine(() -> "Generating columns description for push-down query using "
                + this.columnMetadataReader.getClass().getSimpleName() + ":\n" + query);
        try (final PreparedStatement statement = this.connection.prepareStatement(query)) {
            final ResultSetMetaData metadata = statement.getMetaData();
            final List<DataType> types = mapResultMetadataToExasolDataTypes(metadata);
            final String columnsDescription = createColumnDescriptionFromDataTypes(types);
            LOGGER.fine(() -> "Columns description: " + columnsDescription);
            return columnsDescription;
        } catch (final SQLException exception) {
            throw new SQLException(
                    "Unable to read remote metadata for push-down query trying to generate result columun description.",
                    exception);
        }

    }

    private String createColumnDescriptionFromDataTypes(final List<DataType> types) {
        final StringBuilder builder = new StringBuilder();
        int columnNumber = 1;
        for (final DataType type : types) {
            if (columnNumber > 1) {
                builder.append(", ");
            }
            builder.append("c");
            builder.append(columnNumber);
            builder.append(" ");
            builder.append(type.toString());
            ++columnNumber;
        }
        return builder.toString();
    }

    private List<DataType> mapResultMetadataToExasolDataTypes(final ResultSetMetaData metadata) throws SQLException {
        final int columnCount = metadata.getColumnCount();
        final List<DataType> types = new ArrayList<>(columnCount);
        for (int columnNumber = 1; columnNumber <= columnCount; ++columnNumber) {
            final JdbcTypeDescription jdbcTypeDescription = getJdbcTypeDescription(metadata, columnNumber);
            final DataType type = this.columnMetadataReader.mapJdbcType(jdbcTypeDescription);
            types.add(type);
        }
        return types;
    }

    protected static JdbcTypeDescription getJdbcTypeDescription(final ResultSetMetaData metadata,
            final int columnNumber) throws SQLException {
        final int jdbcType = metadata.getColumnType(columnNumber);
        final int jdbcPrecisions = metadata.getPrecision(columnNumber);
        final int jdbcScales = metadata.getScale(columnNumber);
        return new JdbcTypeDescription(jdbcType, jdbcScales, jdbcPrecisions, 0,
                metadata.getColumnTypeName(columnNumber));
    }
}