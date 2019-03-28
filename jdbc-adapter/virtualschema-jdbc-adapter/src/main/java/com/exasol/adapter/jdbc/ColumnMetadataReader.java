package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.metadata.ColumnMetadata;

public class ColumnMetadataReader {
    public static final String COLUMN_NAME = "COLUMN_NAME";
    public static final String DATA_TYPE = "DATA_TYPE";
    public static final String COLUMN_SIZE = "COLUMN_SIZE";
    private final Connection connection;
    private final SqlDialect sqlDialect;

    public ColumnMetadataReader(final Connection connection, final SqlDialect sqlDialect) {
        this.connection = connection;
        this.sqlDialect = sqlDialect;
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
            throw new RemoteMetadataReaderException("Unable to read column metadata from remote", exception);
        }
        return columns;
    }

    private ColumnMetadata mapColumn(final ResultSet remoteColumn) throws SQLException {
        return this.sqlDialect.mapColumn(remoteColumn);
//        return ColumnMetadata.builder() //
//                .name(readColumnName(remoteColumn)) //
//                .type(mapDatatype(remoteColumn)) //
//                .build();
    }

//    private String readColumnName(final ResultSet remoteColumn) throws SQLException {
//        return remoteColumn.getString(COLUMN_NAME);
//    }
//
//    /**
//     * Parse the data type from a string
//     *
//     * @param string data type name
//     * @return data type
//     */
//    public DataType mapDatatype(final ResultSet remoteColumn) throws SQLException {
//        final String datatypeName = readDatatypeName(remoteColumn);
//        switch (datatypeName) {
//        case "BOOLEAN":
//            return DataType.createBool();
//        case "CHAR":
//            return DataType.createChar(readColumnSize(remoteColumn), ExaCharset.UTF8);
//        case "DATE":
//            return DataType.createDate();
//        case "DOUBLE":
//            return DataType.createDouble();
//        case "TIMESTAMP":
//            return DataType.createTimestamp(false);
//        case "TIMESTAMP WITH LOCAL TIME ZONE":
//            return DataType.createTimestamp(true);
//        case "VARCHAR":
//            return DataType.createVarChar(readColumnSize(remoteColumn), ExaCharset.UTF8);
//        default:
//            throw new IllegalArgumentException("Unable to map \"" + datatypeName + "\" into an Exasol data type.");
//        }
//
//    }
//
//    private int readColumnSize(final ResultSet remoteColumn) throws SQLException {
//        return remoteColumn.getInt(COLUMN_SIZE);
//    }
//
//    private String readDatatypeName(final ResultSet remoteColumn) throws SQLException {
//        return remoteColumn.getString(DATA_TYPE);
//    }
}