package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;

/**
 * This class implements basic reading of database metadata from JDBC
 *
 * <p>
 * See <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html">java.sql.DatabaseMetaData</a>
 */
public class BaseRemoteMetadataReader implements RemoteMetadataReader {
    private final Connection connection;

    public BaseRemoteMetadataReader(final Connection connection) {
        this.connection = connection;
    }

    @Override
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
        throw new RuntimeException("Not implemented yet"); // FIXME: implement
    }

    @Override
    public SchemaAdapterNotes getSchemaAdapterNotes() {
        try {
            final DatabaseMetaData metadata = this.connection.getMetaData();
            return SchemaAdapterNotes.builder() //
                    .catalogSeparator(metadata.getCatalogSeparator()) //
                    .build();
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to create schema adapte notes from remote schema.",
                    exception);
        }
    }
}