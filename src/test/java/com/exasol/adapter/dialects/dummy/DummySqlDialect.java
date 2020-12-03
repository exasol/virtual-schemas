package com.exasol.adapter.dialects.dummy;

import static com.exasol.adapter.AdapterProperties.EXCEPTION_HANDLING_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;

import java.sql.SQLException;
import java.util.Set;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;

public final class DummySqlDialect extends AbstractSqlDialect {
    static final String NAME = "DUMMYDIALECT";

    public DummySqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties, Set.of(SCHEMA_NAME_PROPERTY, EXCEPTION_HANDLING_PROPERTY));
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
    public String getStringLiteral(final String value) {
        return super.quoteLiteralStringWithSingleQuote(value);
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new BaseRemoteMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(
                    "Unable to create remote metadata reader for the Dummy dialect. Caused by: "
                            + exception.getMessage(),
                    exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new ImportIntoQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
    }
}