package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.exasol.adapter.metadata.*;

/**
 * This class implements basic reading of database metadata from JDBC
 *
 * <p>
 * See <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html">java.sql.DatabaseMetaData</a>
 */
public class RemoteMetadataReader {
    private static final Logger LOGGER = Logger.getLogger(RemoteMetadataReader.class.getName());
    private static final String TABLE_NAME_COLUMN = "TABLE_NAME";

    private final Connection connection;

    public RemoteMetadataReader(final Connection connection) {
        this.connection = connection;
    }

    public SchemaMetadata readRemoteSchemaMetadata() {
        try {
            final DatabaseMetaData remoteMetadata = this.connection.getMetaData();
            final String adapterNotes = null;
            final List<TableMetadata> tables = extractTableMetadata(remoteMetadata);
            return new SchemaMetadata(adapterNotes, tables);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to read remote schema metadata.", exception);
        }
    }

    private List<TableMetadata> extractTableMetadata(final DatabaseMetaData remoteMetadata) throws SQLException {
        final List<TableMetadata> translatedTables = new ArrayList<>();
        try (final ResultSet remoteTables = remoteMetadata.getTables(ANY_CATALOG, ANY_SCHEMA, ANY_TABLE, ANY_TYPE)) {
            mapTables(translatedTables, remoteTables);
            return translatedTables;
        }
    }

    private void mapTables(final List<TableMetadata> translatedTables, final ResultSet remoteTables)
            throws SQLException {
        while (remoteTables.next()) {
            final TableMetadata tableMetadata = mapTable(remoteTables);
            translatedTables.add(tableMetadata);
        }
    }

    private TableMetadata mapTable(final ResultSet remoteTable) throws SQLException {
        final String tableName = readTableName(remoteTable);
        LOGGER.info(() -> "Mapping metadata for table \"" + tableName + "\"");
        final String adapterNotes = null;
        final List<ColumnMetadata> columns = new ColumnMetadataReader(this.connection).mapColumns(tableName);
        final String comment = null;
        return new TableMetadata(tableName, adapterNotes, columns, comment);
    }

    private String readTableName(final ResultSet remoteTable) throws SQLException {
        return remoteTable.getString(TABLE_NAME_COLUMN);
    }
}
