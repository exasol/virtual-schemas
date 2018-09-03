package com.exasol.adapter.dialects.impl;

import java.sql.SQLException;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.AbstractSqlDialect;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.exasol.adapter.metadata.DataType;

/**
 * This dialect can be used for data sources where a custom dialect
 * implementation does not yet exists. It will obtain all information from the
 * JDBC Metadata.
 */
public class GenericSqlDialect extends AbstractSqlDialect {

    public GenericSqlDialect(final SqlDialectContext context) {
        super(context);
    }

    private static final String NAME = "GENERIC";

    public static String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities cap = new Capabilities();
        return cap;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.UNKNOWN;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.UNKNOWN;
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        final SchemaAdapterNotes adapterNotes = getContext().getSchemaAdapterNotes();
        if (adapterNotes.isSupportsMixedCaseIdentifiers()) {
            // Unquoted identifiers are treated case-sensitive and stored mixed case
            return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
        } else {
            if (adapterNotes.isStoresLowerCaseIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_LOWER;
            } else if (adapterNotes.isStoresUpperCaseIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_UPPER;
            } else if (adapterNotes.isStoresMixedCaseIdentifiers()) {
                // This case is a bit strange - case insensitive, but still stores it mixed case
                return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
            } else {
                throw new RuntimeException(
                        "Unexpected quote behavior. Adapternotes: " + SchemaAdapterNotes.serialize(adapterNotes));
            }
        }
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        final SchemaAdapterNotes adapterNotes = getContext().getSchemaAdapterNotes();
        if (adapterNotes.isSupportsMixedCaseQuotedIdentifiers()) {
            // Quoted identifiers are treated case-sensitive and stored mixed case
            return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
        } else {
            if (adapterNotes.isStoresLowerCaseQuotedIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_LOWER;
            } else if (adapterNotes.isStoresUpperCaseQuotedIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_UPPER;
            } else if (adapterNotes.isStoresMixedCaseQuotedIdentifiers()) {
                // This case is a bit strange - case insensitive, but still stores it mixed case
                return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
            } else {
                throw new RuntimeException(
                        "Unexpected quote behavior. Adapternotes: " + SchemaAdapterNotes.serialize(adapterNotes));
            }
        }
    }

    @Override
    public String applyQuote(final String identifier) {
        final String quoteString = getContext().getSchemaAdapterNotes().getIdentifierQuoteString();
        return quoteString + identifier + quoteString;
    }

    @Override
    public String applyQuoteIfNeeded(final String identifier) {
        // We could consider getExtraNameCharacters() here as well to do less quoting
        return applyQuote(identifier);
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        // See getCatalogSeparator(): String that this database uses as the separator
        // between a catalog and table name.
        // See isCatalogAtStart(): whether a catalog appears at the start of a fully
        // qualified table name
        return true;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        final SchemaAdapterNotes notes = getContext().getSchemaAdapterNotes();
        if (notes.isNullsAreSortedAtEnd()) {
            return NullSorting.NULLS_SORTED_AT_END;
        } else if (notes.isNullsAreSortedAtStart()) {
            return NullSorting.NULLS_SORTED_AT_START;
        } else if (notes.isNullsAreSortedLow()) {
            return NullSorting.NULLS_SORTED_LOW;
        } else {
            assert (notes.isNullsAreSortedHigh());
            return NullSorting.NULLS_SORTED_HIGH;
        }
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcType) throws SQLException {
        return null;
    }
}
