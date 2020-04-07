package com.exasol.adapter.dialects.dummy;

import static com.exasol.adapter.AdapterProperties.*;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;

public final class DummySqlDialect extends AbstractSqlDialect {
    static final String NAME = "DUMMYDIALECT";
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY,
            PASSWORD_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY, DEBUG_ADDRESS_PROPERTY,
            LOG_LEVEL_PROPERTY, EXCEPTION_HANDLING_PROPERTY);

    public DummySqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties);
    }

    @Override
    public String getName() {
        return NAME;
    }

    public AdapterProperties getProperties() {
        return this.properties;
    }

    @Override
    public Capabilities getCapabilities() {
        return null;
    }

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.NONE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.NONE;
    }

    @Override
    public String applyQuote(final String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return null;
    }

    @Override
    protected List<String> getSupportedProperties() {
        return SUPPORTED_PROPERTIES;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new BaseRemoteMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to create remote metadata reader for the Dummy dialect.",
                    exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
    }
}