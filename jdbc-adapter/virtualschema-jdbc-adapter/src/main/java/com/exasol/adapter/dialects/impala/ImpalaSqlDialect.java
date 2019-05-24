package com.exasol.adapter.dialects.impala;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.RemoteMetadataReader;

/**
 * Dialect for Impala, using the Cloudera Impala JDBC Driver/Connector
 * (developed by Simba).
 * <p>
 * See
 * http://www.cloudera.com/documentation/enterprise/latest/topics/impala_langref.html
 */
public class ImpalaSqlDialect extends AbstractSqlDialect {
    private static final String NAME = "IMPALA";
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
            DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    public static String getPublicName() {
        return NAME;
    }

    public ImpalaSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        builder.addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING,
                ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET);
        builder.addLiteral(NULL, DOUBLE, EXACTNUMERIC, STRING, BOOL);
        builder.addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IN_CONSTLIST,
                IS_NULL, IS_NOT_NULL);
        builder.addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, GROUP_CONCAT, GROUP_CONCAT_SEPARATOR, SUM,
                SUM_DISTINCT, MIN, MAX, AVG);
        return builder.build();
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
    public String applyQuote(final String identifier) {
        return "`" + identifier + "`";
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
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new ImpalaSqlGenerationVisitor(this, context);
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_HIGH;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
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
        return new ImpalaMetadataReader(this.connection, this.properties);
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, this.remoteMetadataReader, this.connection);
    }
}