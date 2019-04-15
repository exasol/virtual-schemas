package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.ANY_TABLE;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;
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
    private final IdentifierConverter identifierConverter;

    /**
     * Create a new instance of a {@link BaseTableMetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public BaseRemoteMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
        this.identifierConverter = createIdentifierConverter();
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
        return new BaseColumnMetadataReader(this.connection, this.properties, this.identifierConverter);
    }

    /**
     * Create a reader that handles table metadata
     * <p>
     * Override this method in cases where a remote data source needs specific handling of table metadata
     *
     * @return table metadata reader
     */
    protected TableMetadataReader createTableMetadataReader() {
        return new BaseTableMetadataReader(this.connection, this.columnMetadataReader, this.properties,
                this.identifierConverter);
    }

    /**
     * Create a converter that translates identifiers from the remote data source to the Exasol representation
     *
     * @return identifier converter
     */
    protected IdentifierConverter createIdentifierConverter() {
        return BaseIdentifierConverter.createDefault();
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

    public IdentifierConverter getIdentifierConverter() {
        return this.identifierConverter;
    }

    @Override
    public SchemaMetadata readRemoteSchemaMetadata() {
        try {
            final String adapterNotes = SchemaAdapterNotesJsonConverter.getInstance()
                    .convertToJson(getSchemaAdapterNotes());
            final DatabaseMetaData remoteMetadata = this.connection.getMetaData();
            final List<TableMetadata> tables = extractTableMetadata(remoteMetadata, Optional.empty());
            return new SchemaMetadata(adapterNotes, tables);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(
                    "Unable to read remote schema metadata. SQL error: " + exception.getMessage(), exception);
        }
    }

    private List<TableMetadata> extractTableMetadata(final DatabaseMetaData remoteMetadata,
            final Optional<List<String>> selectedTables) throws SQLException {
        final String catalogName = getCatalogNameFilter();
        final String schemaName = getSchemaNameFilter();
        logTablesScan(catalogName, schemaName);
        try (final ResultSet remoteTables = remoteMetadata.getTables(catalogName, schemaName, ANY_TABLE,
                getSupportedTableTypes().toArray(new String[0]))) {
            return mapTables(remoteTables, selectedTables);
        }
    }

    @Override
    public Set<String> getSupportedTableTypes() {
        return RemoteMetadataReaderConstants.DEFAULT_SUPPORTED_TABLE_TYPES;
    }

    protected void logTablesScan(final String catalogName, final String schemaName) {
        LOGGER.fine(() -> {
            final StringBuilder builder = new StringBuilder("Scanning \"");
            if (catalogName == null) {
                builder.append("any catalog, ");
            } else {
                builder.append("catalog \"");
                builder.append(catalogName);
                builder.append("\", ");
            }
            if (schemaName == null) {
                builder.append("any schema ");
            } else {
                builder.append("schema \"");
                builder.append(schemaName);
                builder.append("\" ");
            }
            builder.append("for contained tables.");
            return builder.toString();
        });
    }

    private List<TableMetadata> mapTables(final ResultSet remoteTables, final Optional<List<String>> selectedTables)
            throws SQLException {
        return this.tableMetadataReader.mapTables(remoteTables, selectedTables);
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
            throw new RemoteMetadataReaderException("Unable to create schema adapter notes from remote schema.",
                    exception);
        }
    }

    @Override
    public SchemaMetadata readRemoteSchemaMetadata(final List<String> selectedTables) {
        try {
            final DatabaseMetaData remoteMetadata = this.connection.getMetaData();
            final String adapterNotes = SchemaAdapterNotesJsonConverter.getInstance()
                    .convertToJson(getSchemaAdapterNotes());
            final List<TableMetadata> tables = extractTableMetadata(remoteMetadata, Optional.of(selectedTables));
            return new SchemaMetadata(adapterNotes, tables);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to read remote schema metadata.", exception);
        }
    }
}