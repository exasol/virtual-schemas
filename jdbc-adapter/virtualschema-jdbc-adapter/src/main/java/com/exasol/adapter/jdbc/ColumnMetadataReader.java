package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.DataType.ExaCharset;

public class ColumnMetadataReader {
    public static final int COLUMN_NAME = 4;
    public static final int DATA_TYPE = 5;
    public static final int COLUMN_SIZE = 7;
    private final Connection connection;

    public ColumnMetadataReader(final Connection connection) {
        this.connection = connection;
    }

    public List<ColumnMetadata> mapColumns(final String tableName) {
        final List<ColumnMetadata> columns = new ArrayList<>();
        try (final ResultSet remoteColumns = this.connection.getMetaData().getColumns(ANY_CATALOG, ANY_SCHEMA,
                tableName, ANY_COLUMN)) {
            while (remoteColumns.next()) {
                final ColumnMetadata metadata = mapColumn(remoteColumns);
                columns.add(metadata);
            }
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to read table metadata from remote", exception);
        }
        return columns;
    }

    private ColumnMetadata mapColumn(final ResultSet remoteColumn) throws SQLException {
        return ColumnMetadata.builder() //
                .name(readColumnName(remoteColumn)) //
                .type(mapDatatype(remoteColumn)) //
                .build();
    }

    private String readColumnName(final ResultSet remoteColumn) {
        try {
            return remoteColumn.getString(COLUMN_NAME);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to read remote column name.", exception);
        }
    }

    /**
     * Parse the data type from a string
     *
     * @param string data type name
     * @return data type
     */
    public DataType mapDatatype(final ResultSet remoteColumn) throws SQLException {
        final String datatypeName = readDatatypeName(remoteColumn);
        switch (datatypeName) {
        case "BOOLEAN":
            return DataType.createBool();
        case "CHAR":
            return DataType.createChar(readColumnSize(remoteColumn), ExaCharset.UTF8);
        case "DOUBLE":
            return DataType.createDouble();
        default:
            throw new IllegalArgumentException("Unable to map \"" + datatypeName + "\" into an Exasol data type.");
        }

    }

    private int readColumnSize(final ResultSet remoteColumn) throws SQLException {
        return remoteColumn.getInt(COLUMN_SIZE);
    }

    private String readDatatypeName(final ResultSet remoteColumn) throws SQLException {
        return remoteColumn.getString(DATA_TYPE);
    }
}