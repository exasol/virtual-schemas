package com.exasol.adapter.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect.IdentifierCaseHandling;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;
import com.google.common.base.Strings;

/**
 * This class maps metadata of tables from the remote source to Exasol
 */
public class BaseTableMetadataReader implements TableMetadataReader {
    static final String NAME_COLUMN = "TABLE_NAME";
    static final String REMARKS_COLUMN = "REMARKS";
    static final String DEFAULT_TABLE_ADAPTER_NOTES = "";
    private static final Logger LOGGER = Logger.getLogger(BaseTableMetadataReader.class.getName());
    private static final Pattern UNQUOTED_IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z]\\w*");
    private final ColumnMetadataReader columnMetadataReader;
    protected final AdapterProperties properties;

    /**
     * Create a new instance of a {@link TableMetadata}
     *
     * @param columnMetadataReader reader to be used to map the tables columns
     */
    public BaseTableMetadataReader(final ColumnMetadataReader columnMetadataReader,
            final AdapterProperties properties) {
        this.columnMetadataReader = columnMetadataReader;
        this.properties = properties;
    }

    @Override
    public List<TableMetadata> mapTables(final ResultSet remoteTables) throws SQLException {
        final List<TableMetadata> translatedTables = new ArrayList<>();
        while (remoteTables.next()) {
            mapOrSkipTable(remoteTables, translatedTables);
        }
        return translatedTables;
    }

    protected void mapOrSkipTable(final ResultSet remoteTables, final List<TableMetadata> translatedTables)
            throws SQLException {
        final String tableName = readTableName(remoteTables);
        if (isTableIncludedByMapping(tableName)) {
            mapOrSkipSupportedTable(remoteTables, translatedTables, tableName);
        } else {
            skipUnsupportedTable(tableName);
        }
    }

    @Override
    public boolean isTableIncludedByMapping(final String tableName) {
        return true;
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
        final String comment = Strings.nullToEmpty(readComment(table));
        final List<ColumnMetadata> columns = this.columnMetadataReader.mapColumns(tableName);
        final String adapterNotes = DEFAULT_TABLE_ADAPTER_NOTES;
        return new TableMetadata(adjustIdentifierCase(tableName), adapterNotes, columns, comment);
    }

    protected String readTableName(final ResultSet remoteTables) throws SQLException {
        return remoteTables.getString(NAME_COLUMN);
    }

    protected String readComment(final ResultSet remoteTables) throws SQLException {
        return remoteTables.getString(REMARKS_COLUMN);
    }

    @Override
    public String adjustIdentifierCase(final String identifier) {
        if (getQuotedIdentifierCaseHandling() == this.getUnquotedIdentifierCaseHandling()) {
            if (isQuotedIdentifierCaseSensitive()) {
                return identifier.toUpperCase();
            } else {
                return identifier;
            }
        }
        return identifier;
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierCaseHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_UPPER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierCaseHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }

    protected boolean isQuotedIdentifierCaseSensitive() {
        return getQuotedIdentifierCaseHandling() != IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }

    protected boolean isUnquotedIdentifier(final String identifier) {
        return UNQUOTED_IDENTIFIER_PATTERN.matcher(identifier).matches();
    }

    protected void skipUnsupportedTable(final String tableName) {
        LOGGER.fine(() -> "Skipping unsupported table \"" + tableName + "\" when mapping remote metadata.");
    }
}