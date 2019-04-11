package com.exasol.adapter.dialects.impl;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.AbstractSqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.jdbc.SchemaAdapterNotes;

/**
 * This dialect can be used for data sources where a custom dialect implementation does not yet exists. It will obtain
 * all information from the JDBC Metadata.
 */
public class GenericSqlDialect extends AbstractSqlDialect {
    public GenericSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    private static final String NAME = "GENERIC";

    public static String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        return builder.build();
    }

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.AUTO_DETECT;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.AUTO_DETECT;
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        final SchemaAdapterNotes adapterNotes = this.remoteMetadataReader.getSchemaAdapterNotes();
        if (adapterNotes.supportsMixedCaseIdentifiers()) {
            // Unquoted identifiers are treated case-sensitive and stored mixed case
            return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
        } else {
            if (adapterNotes.storesLowerCaseIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_LOWER;
            } else if (adapterNotes.storesUpperCaseIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_UPPER;
            } else if (adapterNotes.storesMixedCaseIdentifiers()) {
                // This case is a bit strange - case insensitive, but still stores it mixed case
                return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
            } else {
                throw new UnsupportedOperationException("Unexpected quote behavior. Adapter notes: " //
                        + adapterNotes.toString());
            }
        }
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        final SchemaAdapterNotes adapterNotes = this.remoteMetadataReader.getSchemaAdapterNotes();
        if (adapterNotes.supportsMixedCaseQuotedIdentifiers()) {
            // Quoted identifiers are treated case-sensitive and stored mixed case
            return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
        } else {
            if (adapterNotes.storesLowerCaseQuotedIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_LOWER;
            } else if (adapterNotes.storesUpperCaseQuotedIdentifiers()) {
                return IdentifierCaseHandling.INTERPRET_AS_UPPER;
            } else if (adapterNotes.storesMixedCaseQuotedIdentifiers()) {
                // This case is a bit strange - case insensitive, but still stores it mixed case
                return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
            } else {
                throw new UnsupportedOperationException("Unexpected quote behavior. Adapter notes: " //
                        + adapterNotes.toString());
            }
        }
    }

    @Override
    public String applyQuote(final String identifier) {
        final String quoteString = this.remoteMetadataReader.getSchemaAdapterNotes().getIdentifierQuoteString();
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
        final SchemaAdapterNotes notes = this.remoteMetadataReader.getSchemaAdapterNotes();
        if (notes.areNullsSortedAtEnd()) {
            return NullSorting.NULLS_SORTED_AT_END;
        } else if (notes.areNullsSortedAtStart()) {
            return NullSorting.NULLS_SORTED_AT_START;
        } else if (notes.areNullsSortedLow()) {
            return NullSorting.NULLS_SORTED_LOW;
        } else {
            assert (notes.areNullsSortedHigh());
            return NullSorting.NULLS_SORTED_HIGH;
        }
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
