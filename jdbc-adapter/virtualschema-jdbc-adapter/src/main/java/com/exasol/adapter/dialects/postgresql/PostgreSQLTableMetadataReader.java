package com.exasol.adapter.dialects.postgresql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.PostgreSQLIdentifierMapping;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.metadata.TableMetadata;

/**
 * This class handles the specifics of mapping PostgreSQL table metadata to Exasol
 */
public class PostgreSQLTableMetadataReader extends BaseTableMetadataReader {
    static final String POSTGRESQL_UPPERCASE_TABLES_SWITCH = "POSTGRESQL_UPPERCASE_TABLES";
    static final String IGNORE_ERRORS_PROPERTY = "IGNORE_ERRORS";
    static final String POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY = "POSTGRESQL_IDENTIFIER_MAPPING";

    /**
     * Create a new {@link PostgreSQLTableMetadataReader} instance
     *
     * @param columnMetadataReader reader to be used to map the metadata of the tables columns
     */
    public PostgreSQLTableMetadataReader(final ColumnMetadataReader columnMetadataReader,
            final AdapterProperties properties) {
        super(columnMetadataReader, properties);
    }

    /**
     * Get the identifier mapping that the metadata reader uses when mapping PostgreSQL tables to Exasol
     *
     * @return identifier mapping
     */
    public PostgreSQLIdentifierMapping getIdentifierMapping() {
        return PostgreSQLIdentifierMapping.valueOf(this.properties.get(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY));
    }

    /**
     * Check if the metadata reader should ignore tables where the name contains upper-case characters
     *
     * @return <code>true</code> if the reader should ignore upper-case tables
     */
    public boolean ignoresUpperCaseTables() {
        return this.properties.getIgnoredErrors().contains(POSTGRESQL_UPPERCASE_TABLES_SWITCH);
    }

    @Override
    public List<TableMetadata> mapTables(final ResultSet remoteTables, final Optional<List<String>> selectedTables)
            throws SQLException {
        final String tableName = readTableName(remoteTables);
        if (isTableSelected(tableName, selectedTables) //
                && (getIdentifierMapping() == PostgreSQLIdentifierMapping.CONVERT_TO_UPPER) //
                && !ignoresUpperCaseTables() //
                && containsUppercaseCharacter(tableName)) {
            throw new RemoteMetadataReaderException("Table \"" + tableName
                    + "\" cannot be used in virtual schema. Set property " + IGNORE_ERRORS_PROPERTY + " to "
                    + POSTGRESQL_UPPERCASE_TABLES_SWITCH + " to enforce schema creation.");
        } else {
            return super.mapTables(remoteTables, selectedTables);
        }
    }

    private boolean containsUppercaseCharacter(final String tableName) {
        for (int i = 0; i < tableName.length(); i++) {
            if (Character.isUpperCase(tableName.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String adjustIdentifierCase(final String identifier) {
        if (getIdentifierMapping() != PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE) {
            if (isUnquotedIdentifier(identifier)) {
                return identifier.toUpperCase();
            } else {
                return identifier;
            }
        }
        return identifier;
    }
}