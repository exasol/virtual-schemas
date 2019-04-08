package com.exasol.adapter.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect.IdentifierCaseHandling;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.TableMetadata;

/**
 * This class maps metadata of tables from the remote source to Exasol
 */
public class BaseTableMetadataReader implements TableMetadataReader {
    static final String NAME_COLUMN = "TABLE_NAME";
    static final String REMARKS_COLUMN = "REMARKS";
    static final String DEFAULT_TABLE_ADAPTER_NOTES = "";
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
            final TableMetadata tableMetadata = mapTable(remoteTables);
            translatedTables.add(tableMetadata);
        }
        return translatedTables;
    }

    protected TableMetadata mapTable(final ResultSet remoteTables) throws SQLException {
        final String tableName = readTableName(remoteTables);
        final String comment = readComment(remoteTables);
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
}