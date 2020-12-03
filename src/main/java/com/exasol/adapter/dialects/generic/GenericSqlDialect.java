package com.exasol.adapter.dialects.generic;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;

import java.sql.SQLException;
import java.util.Set;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.adapternotes.SchemaAdapterNotes;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;

/**
 * This dialect can be used for data sources where a custom dialect implementation does not yet exists. It will obtain
 * all information from the JDBC Metadata.
 */
public class GenericSqlDialect extends AbstractSqlDialect {
    static final String NAME = "GENERIC";
    private final RemoteMetadataReader remoteMetadataReader;

    /**
     * Create a new instance of the {@link GenericSqlDialect}.
     *
     * @param connectionFactory factory for the JDBC connection to the remote data source
     * @param properties        user-defined adapter properties
     */
    public GenericSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties, Set.of(CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY));
        this.remoteMetadataReader = createRemoteMetadataReader();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        return Capabilities.builder().build();
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
    public String applyQuote(final String identifier) {
        final String quoteString = this.remoteMetadataReader.getSchemaAdapterNotes().getIdentifierQuoteString();
        if (identifier.contains(quoteString)) {
            throw new IllegalArgumentException("An identifier '" + identifier + "' contains illegal substring: '"
                    + quoteString + "'. Please remove it to use the generic dialect.");
        }
        return quoteString + identifier + quoteString;
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
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
        return this.quoteLiteralStringWithSingleQuote(value);
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new GenericMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(
                    "Unable to create remote metadata reader for the generic SQL dialect. Caused by: "
                            + exception.getMessage(),
                    exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new ImportIntoQueryRewriter(this, this.remoteMetadataReader, this.connectionFactory);
    }
}