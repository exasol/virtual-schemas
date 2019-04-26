package com.exasol.adapter.dialects.postgresql;

import java.util.logging.Logger;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.PostgreSQLIdentifierMapping;
import com.exasol.adapter.jdbc.*;

/**
 * This class handles the specifics of mapping PostgreSQL table metadata to Exasol
 */
public class PostgreSQLTableMetadataReader extends BaseTableMetadataReader {
    static final Logger LOGGER = Logger.getLogger(PostgreSQLTableMetadataReader.class.getName());
    static final String POSTGRESQL_UPPERCASE_TABLES_SWITCH = "POSTGRESQL_UPPERCASE_TABLES";
    static final String IGNORE_ERRORS_PROPERTY = "IGNORE_ERRORS";
    static final String POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY = "POSTGRESQL_IDENTIFIER_MAPPING";

    /**
     * Create a new {@link PostgreSQLTableMetadataReader} instance
     *
     * @param columnMetadataReader reader to be used to map the metadata of the tables columns
     * @param properties           user-defined adapter properties
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
        return this.properties.containsKey(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY) //
                ? PostgreSQLIdentifierMapping.valueOf(this.properties.get(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY))
                : PostgreSQLIdentifierMapping.CONVERT_TO_UPPER;
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
    public boolean isTableIncludedByMapping(final String tableName) {
        if (containsUppercaseCharacter(tableName) && !isUnquotedIdentifier(tableName)) {
            return isUppercaseTableIncludedByMapping(tableName);
        } else {
            return true;
        }
    }

    protected boolean isUppercaseTableIncludedByMapping(final String tableName) {
        if (getIdentifierMapping() == PostgreSQLIdentifierMapping.CONVERT_TO_UPPER) {
            if (ignoresUpperCaseTables()) {
                LOGGER.fine(() -> "Ignoring PostgreSQL table \"" + tableName + "\""
                        + "because it contains an uppercase character and " + IGNORE_ERRORS_PROPERTY + " is set to \""
                        + POSTGRESQL_UPPERCASE_TABLES_SWITCH + "\".");
                return false;
            } else {
                throw new RemoteMetadataReaderException("Table \"" + tableName
                        + "\" cannot be used in virtual schema. Set property " + IGNORE_ERRORS_PROPERTY + " to \""
                        + POSTGRESQL_UPPERCASE_TABLES_SWITCH + "\" to enforce schema creation.");
            }
        } else {
            return true;
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
        if (getIdentifierMapping() == PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE) {
            return identifier;
        } else {
            if (isUnquotedIdentifier(identifier)) {
                return identifier.toUpperCase();
            } else {
                return identifier;
            }
        }
    }
}