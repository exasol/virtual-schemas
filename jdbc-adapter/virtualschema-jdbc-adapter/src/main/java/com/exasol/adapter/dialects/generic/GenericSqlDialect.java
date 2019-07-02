package com.exasol.adapter.dialects.generic;

import static com.exasol.adapter.AdapterProperties.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.jdbc.SchemaAdapterNotes;

/**
 * This dialect can be used for data sources where a custom dialect implementation does not yet exists. It will obtain
 * all information from the JDBC Metadata.
 */
public class GenericSqlDialect extends AbstractSqlDialect {
    private static final String NAME = "GENERIC";
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
            DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    /**
     * Create a new instance of the {@link GenericSqlDialect}.
     *
     * @param connection SQL connection
     * @param properties user-defined adapter properties
     */
    public GenericSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    /**
     * Get the Generic dialect name.
     *
     * @return always "GENERIC"
     */
    public static String getPublicName() {
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
    public void validateProperties() throws PropertyValidationException {
        super.validateDialectName(getPublicName());
        super.validateProperties();
    }

    @Override
    protected List<String> getSupportedProperties() {
        return SUPPORTED_PROPERTIES;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new GenericMetadataReader(this.connection, this.properties);
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, this.remoteMetadataReader, this.connection);
    }
}
