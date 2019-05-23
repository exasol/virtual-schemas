package com.exasol.adapter.dialects.exasol;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.dialects.exasol.ExasolProperties.EXASOL_CONNECTION_STRING_PROPERTY;
import static com.exasol.adapter.dialects.exasol.ExasolProperties.EXASOL_IMPORT_PROPERTY;
import static com.exasol.adapter.sql.ScalarFunction.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.RemoteMetadataReader;

/**
 * Exasol SQL dialect
 */
public class ExasolSqlDialect extends AbstractSqlDialect {
    private static final String NAME = "EXASOL";

    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXASOL_IMPORT_PROPERTY,
            EXASOL_CONNECTION_STRING_PROPERTY, IS_LOCAL_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
            DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    public ExasolSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
        this.omitParenthesesMap.add(SYSDATE);
        this.omitParenthesesMap.add(SYSTIMESTAMP);
        this.omitParenthesesMap.add(CURRENT_SCHEMA);
        this.omitParenthesesMap.add(CURRENT_SESSION);
        this.omitParenthesesMap.add(CURRENT_STATEMENT);
        this.omitParenthesesMap.add(CURRENT_USER);
    }

    /**
     * Get the name under which the dialect is listed.
     *
     * @return name of the dialect
     */
    public static String getPublicName() {
        return NAME;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new ExasolMetadataReader(this.connection, this.properties);
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new ExasolQueryRewriter(this, this.remoteMetadataReader, this.connection);
    }

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.SINGLE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        builder.addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING,
                ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET, JOIN, JOIN_TYPE_INNER,
                JOIN_TYPE_LEFT_OUTER, JOIN_TYPE_RIGHT_OUTER, JOIN_TYPE_FULL_OUTER, JOIN_CONDITION_EQUI);
        builder.addLiteral(LiteralCapability.values());
        builder.addPredicate(PredicateCapability.values());
        builder.addAggregateFunction(AggregateFunctionCapability.values());
        builder.addScalarFunction(ScalarFunctionCapability.values());
        return builder.build();
    }

    @Override
    public String applyQuote(final String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_HIGH;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /**
     * Return the type of import the Exasol dialect uses
     *
     * @return import type
     */
    public ImportType getImportType() {
        if (this.properties.isEnabled(IS_LOCAL_PROPERTY)) {
            return ImportType.LOCAL;
        } else if (this.properties.isEnabled(EXASOL_IMPORT_PROPERTY)) {
            return ImportType.EXA;
        } else {
            return ImportType.JDBC;
        }
    }

    @Override
    public void validateProperties() throws PropertyValidationException {
        super.validateDialectName(getPublicName());
        super.validateProperties();
        super.checkImportPropertyConsistency(EXASOL_IMPORT_PROPERTY, EXASOL_CONNECTION_STRING_PROPERTY);
        super.validateBooleanProperty(EXASOL_IMPORT_PROPERTY);
        super.validateBooleanProperty(IS_LOCAL_PROPERTY);
    }

    @Override
    protected List<String> getSupportedProperties() {
        return SUPPORTED_PROPERTIES;
    }
}