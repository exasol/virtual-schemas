package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.*;

import java.sql.*;
import java.util.List;

import com.exasol.adapter.AdapterProperties;
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
    private final AdapterProperties properties;

    /**
     * Create a new instance of a {@link BaseTableMetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public BaseRemoteMetadataReader(final Connection connection, final AdapterProperties properties) {
        this.connection = connection;
        this.properties = properties;
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
        try (final ResultSet remoteTables = remoteMetadata.getTables(ANY_CATALOG, ANY_SCHEMA, ANY_TABLE, ANY_TYPE)) {
            return mapTables(remoteTables);
        }
    }

    private List<TableMetadata> mapTables(final ResultSet remoteTables) throws SQLException {
        final ColumnMetadataReader columnMetadataReader = new ColumnMetadataReader(this.connection);
        final TableMetadataReader tableMetadataReader = new BaseTableMetadataReader(columnMetadataReader,
                this.properties);
        return tableMetadataReader.mapTables(remoteTables);
    }

    @Override
    public SchemaAdapterNotes getSchemaAdapterNotes() {
        try {
            final DatabaseMetaData metadata = this.connection.getMetaData();
            return SchemaAdapterNotes.builder() //
                    .catalogSeparator(metadata.getCatalogSeparator()) //
                    .identifierQuoteString(metadata.getIdentifierQuoteString()) //
                    .storesLowerCaseIdentifiers(metadata.storesLowerCaseIdentifiers()) //
                    .storesUpperCaseIdentifiers(metadata.storesUpperCaseIdentifiers()) //
                    .storesMixedCaseIdentifiers(metadata.storesMixedCaseIdentifiers()) //
                    .supportsMixedCaseIdentifiers(metadata.supportsMixedCaseIdentifiers()) //
                    .storesLowerCaseQuotedIdentifiers(metadata.storesLowerCaseQuotedIdentifiers()) //
                    .storesUpperCaseQuotedIdentifiers(metadata.storesUpperCaseQuotedIdentifiers()) //
                    .storesMixedCaseQuotedIdentifiers(metadata.storesMixedCaseQuotedIdentifiers()) //
                    .supportsMixedCaseQuotedIdentifiers(metadata.supportsMixedCaseQuotedIdentifiers()) //
                    .areNullsSortedAtEnd(metadata.nullsAreSortedAtEnd()) //
                    .areNullsSortedAtStart(metadata.nullsAreSortedAtStart()) //
                    .areNullsSortedHigh(metadata.nullsAreSortedHigh()) //
                    .areNullsSortedLow(metadata.nullsAreSortedLow()) //
                    .build();
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to create schema adapte notes from remote schema.",
                    exception);
        }
    }
}