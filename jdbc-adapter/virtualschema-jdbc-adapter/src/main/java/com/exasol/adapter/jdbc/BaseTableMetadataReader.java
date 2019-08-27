package com.exasol.adapter.jdbc;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;

/**
 * This class maps metadata of tables from the remote source to Exasol.
 */
public class BaseTableMetadataReader extends AbstractMetadataReader implements TableMetadataReader {
    static final String NAME_COLUMN = "TABLE_NAME";
    static final String REMARKS_COLUMN = "REMARKS";
    static final String DEFAULT_TABLE_ADAPTER_NOTES = "";
    private static final Logger LOGGER = Logger.getLogger(BaseTableMetadataReader.class.getName());
    private static final Pattern UNQUOTED_IDENTIFIER_PATTERN = Pattern.compile("^[a-z][0-9a-z_]*");
    protected ColumnMetadataReader columnMetadataReader;
    private final IdentifierConverter identifierConverter;

    /**
     * Create a new instance of a {@link TableMetadata}.
     *
     * @param connection           JDBC connection to remote data source
     * @param columnMetadataReader reader to be used to map the tables columns
     * @param properties           user-defined adapter properties
     * @param identifierConverter  converter between source and Exasol identifiers
     */
    public BaseTableMetadataReader(final Connection connection, final ColumnMetadataReader columnMetadataReader,
            final AdapterProperties properties, final IdentifierConverter identifierConverter) {
        super(connection, properties);
        this.columnMetadataReader = columnMetadataReader;
        this.identifierConverter = identifierConverter;
    }

    @Override
    public List<TableMetadata> mapTables(final ResultSet remoteTables, final Optional<List<String>> selectedTables)
            throws SQLException {
        final List<TableMetadata> translatedTables = new ArrayList<>();
        while (remoteTables.next()) {
            mapOrSkipTable(remoteTables, translatedTables, selectedTables);
        }
        return translatedTables;
    }

    protected void mapOrSkipTable(final ResultSet remoteTables, final List<TableMetadata> translatedTables,
            final Optional<List<String>> selectedTables) throws SQLException {
        final String tableName = readTableName(remoteTables);
        if (isTableIncludedByMapping(tableName) && isTableSelected(tableName, selectedTables)) {
            mapOrSkipSupportedTable(remoteTables, translatedTables, tableName);
        } else {
            skipUnsupportedTable(tableName);
        }
    }

    @Override
    public boolean isTableIncludedByMapping(final String tableName) {
        return true;
    }

    protected boolean isTableSelected(final String tableName, final Optional<List<String>> selectedTables) {
        return !selectedTables.isPresent() || selectedTables.get().contains(tableName);
    }

    protected void mapOrSkipSupportedTable(final ResultSet remoteTables, final List<TableMetadata> translatedTables,
            final String tableName) throws SQLException {
        if (isTableFilteredOut(tableName)) {
            skipFilteredTable(tableName);
        } else {
            mapTableNotFilteredOut(remoteTables, translatedTables, tableName);
        }
    }

    private boolean isTableFilteredOut(final String tableName) {
        return this.properties.getFilteredTables().contains(tableName);
    }

    protected void skipFilteredTable(final String tableName) {
        LOGGER.fine(
                () -> "Skipping table \"" + tableName + "\" when mapping remote data due to user-defined table filter");
    }

    protected void mapTableNotFilteredOut(final ResultSet remoteTables, final List<TableMetadata> translatedTables,
            final String tableName) throws SQLException {
        final TableMetadata tableMetadata = mapTable(remoteTables, tableName);
        if (tableMetadata.getColumns().isEmpty()) {
            skipTableWithEmptyColumns(tableName);
        } else {
            addMappedTable(translatedTables, tableMetadata);
        }
    }

    protected void skipTableWithEmptyColumns(final String tableName) {
        LOGGER.fine(() -> "Not mapping table \"" + tableName + "\" because it has no columns."
                + " This can happen if the view containing the columns is invalid"
                + " or if the Virtual Schema adapter does not support mapping the column types.");
    }

    protected void addMappedTable(final List<TableMetadata> translatedTables, final TableMetadata tableMetadata) {
        LOGGER.finer(() -> "Read table metadata: " + tableMetadata.describe());
        translatedTables.add(tableMetadata);
    }

    protected TableMetadata mapTable(final ResultSet table, final String tableName) throws SQLException {
        final String comment = Optional.ofNullable(readComment(table)).orElse("");
        final List<ColumnMetadata> columns = this.columnMetadataReader.mapColumns(tableName);
        final String adapterNotes = DEFAULT_TABLE_ADAPTER_NOTES;
        return new TableMetadata(adjustIdentifierCase(tableName), adapterNotes, columns, comment);
    }

    private String adjustIdentifierCase(final String tableName) {
        return this.identifierConverter.convert(tableName);
    }

    protected String readTableName(final ResultSet remoteTables) throws SQLException {
        return remoteTables.getString(NAME_COLUMN);
    }

    protected String readComment(final ResultSet remoteTables) throws SQLException {
        return remoteTables.getString(REMARKS_COLUMN);
    }

    protected boolean isUnquotedIdentifier(final String identifier) {
        return UNQUOTED_IDENTIFIER_PATTERN.matcher(identifier).matches();
    }

    protected void skipUnsupportedTable(final String tableName) {
        LOGGER.fine(() -> "Skipping unsupported table \"" + tableName + "\" when mapping remote metadata.");
    }
}