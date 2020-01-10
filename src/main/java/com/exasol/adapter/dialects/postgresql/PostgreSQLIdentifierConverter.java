package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.dialects.postgresql.PostgreSQLSqlDialect.POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY;

import java.util.regex.Pattern;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierCaseHandling;
import com.exasol.adapter.dialects.IdentifierConverter;

/**
 * This class implements database identifier converter for {@link PostgreSQLSqlDialect}.
 */
public class PostgreSQLIdentifierConverter implements IdentifierConverter {
    private static final Pattern UNQUOTED_IDENTIFIER_PATTERN = Pattern.compile("^[a-z][0-9a-z_]*");
    private final AdapterProperties properties;

    /**
     * Create a new instance of the {@link PostgreSQLIdentifierConverter}.
     * 
     * @param properties adapter properties
     */
    public PostgreSQLIdentifierConverter(final AdapterProperties properties) {
        this.properties = properties;
    }

    @Override
    public String convert(final String identifier) {
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

    /**
     * Get the identifier mapping that the metadata reader uses when mapping PostgreSQL tables to Exasol.
     *
     * @return identifier mapping
     */
    public PostgreSQLIdentifierMapping getIdentifierMapping() {
        return this.properties.containsKey(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY) //
                ? PostgreSQLIdentifierMapping.valueOf(this.properties.get(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY))
                : PostgreSQLIdentifierMapping.CONVERT_TO_UPPER;
    }

    private boolean isUnquotedIdentifier(final String identifier) {
        return UNQUOTED_IDENTIFIER_PATTERN.matcher(identifier).matches();
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_LOWER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }
}