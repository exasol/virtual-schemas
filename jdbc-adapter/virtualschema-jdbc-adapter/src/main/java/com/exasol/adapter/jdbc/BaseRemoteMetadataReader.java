package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.ANY_TABLE;
import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.SUPPORTED_TABLE_TYPES;

import java.sql.*;
import java.util.List;
import java.util.logging.Logger;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.TableMetadata;

/**
 * This class implements basic reading of database metadata from JDBC
 *
 * <p>
 * See <a href="https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html">java.sql.DatabaseMetaData</a>
 */
public class BaseRemoteMetadataReader extends AbstractMetadataReader implements RemoteMetadataReader {
    private static final Logger LOGGER = Logger.getLogger(BaseRemoteMetadataReader.class.getName());
    private final ColumnMetadataReader columnMetadataReader;
    private final TableMetadataReader tableMetadataReader;

    /**
     * Create a new instance of a {@link BaseTableMetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public BaseRemoteMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
        this.columnMetadataReader = createColumnMetadataReader();
        this.tableMetadataReader = createTableMetadataReader();
    }

    /**
     * Create a reader that handles column metadata
     * <p>
     * Override this method in cases where a remote data source needs specific handling of column metadata
     *
     * @return column metadata reader
     */
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new BaseColumnMetadataReader(this.connection, this.properties);
    }

    /**
     * Create a reader that handles table metadata
     * <p>
     * Override this method in cases where a remote data source needs specific handling of table metadata
     *
     * @return table metadata reader
     */
    protected TableMetadataReader createTableMetadataReader() {
        return new BaseTableMetadataReader(this.columnMetadataReader, this.properties);
    }

    /**
     * Get the remote column metadata reader
     *
     * @return column metadata reader
     */
    @Override
    public final ColumnMetadataReader getColumnMetadataReader() {
        return this.columnMetadataReader;
    }

    /**
     * Get the table metadata reader
     *
     * @return table metadata reader
     */
    @Override
    public final TableMetadataReader getTableMetadataReader() {
        return this.tableMetadataReader;
    }

    @Override
    public SchemaMetadata readRemoteSchemaMetadata() {
        try {
            final DatabaseMetaData remoteMetadata = this.connection.getMetaData();
            final String adapterNotes = SchemaAdapterNotesJsonConverter.getInstance()
                    .convertToJson(getSchemaAdapterNotes());
            final List<TableMetadata> tables = extractTableMetadata(remoteMetadata);
            return new SchemaMetadata(adapterNotes, tables);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to read remote schema metadata.", exception);
        }
    }

    private List<TableMetadata> extractTableMetadata(final DatabaseMetaData remoteMetadata) throws SQLException {
        final String catalogName = getCatalogName();
        final String schemaName = getSchemaName();
        logTablesScan(catalogName, schemaName);
        try (final ResultSet remoteTables = remoteMetadata.getTables(catalogName, schemaName, ANY_TABLE,
                SUPPORTED_TABLE_TYPES.toArray(new String[0]))) {
            return mapTables(remoteTables);
        }
    }

    protected void logTablesScan(final String catalogName, final String schemaName) {
        LOGGER.fine(
                () -> "Scanning catalog \"" + catalogName + "\", schema \"" + schemaName + "\" for contained tables.");
    }

    private List<TableMetadata> mapTables(final ResultSet remoteTables) throws SQLException {
        return this.tableMetadataReader.mapTables(remoteTables);
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

    @Override
    public SchemaMetadata readRemoteSchemaMetadata(final List<String> tables) {
        // FIXME Auto-generated method stub
        throw new UnsupportedOperationException("Filtering tables not implemented.");
    }
}