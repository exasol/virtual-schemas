package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;

public class ColumnMetadataReader {
    private static final String COLUMN_NAME_COLUMN = "COLUMN_NAME";
    private final Connection connection;

    public ColumnMetadataReader(final Connection connection) {
        this.connection = connection;
    }

    public List<ColumnMetadata> mapColumns(final String tableName) throws SQLException {
        final List<ColumnMetadata> columns = new ArrayList<>();
        try (final ResultSet remoteColumns = this.connection.getMetaData().getColumns(ANY_CATALOG, ANY_SCHEMA,
                tableName, ANY_COLUMN)) {
            while (remoteColumns.next()) {
                final ColumnMetadata metadata = mapColumn(remoteColumns);
                columns.add(metadata);
            }
        }
        return columns;
    }

    private ColumnMetadata mapColumn(final ResultSet remoteColumn) throws SQLException {
        return ColumnMetadata.builder() //
                .name(remoteColumn.getString(COLUMN_NAME_COLUMN)) //
                .type(DataType.createBool()) // FIXME
                .build();
    }
}
