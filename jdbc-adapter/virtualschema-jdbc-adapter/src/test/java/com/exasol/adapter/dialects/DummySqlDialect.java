package com.exasol.adapter.dialects;

import static com.exasol.adapter.AdapterProperties.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.jdbc.RemoteMetadataReader;

public class DummySqlDialect extends AbstractSqlDialect {
    public static String getPublicName() {
        return "dummy dialect";
    }

    private static final List<String> supportedProperties = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY,
            PASSWORD_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY, DEBUG_ADDRESS_PROPERTY,
            LOG_LEVEL_PROPERTY, EXCEPTION_HANDLING_PROPERTY);

    public DummySqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    public AdapterProperties getProperties() {
        return this.properties;
    }

    public RemoteMetadataReader getRemoteMetadataReader() {
        return this.remoteMetadataReader;
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
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return null;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return null;
    }

    @Override
    public String applyQuote(final String identifier) {
        return null;
    }

    @Override
    public String applyQuoteIfNeeded(final String identifier) {
        return null;
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
        return null;
    }

    @Override
    public void validateProperties() throws PropertyValidationException {
        super.validateDialectName("GENERIC");
        super.validateSupportedPropertiesList(supportedProperties);
        super.validateProperties();
    }
}