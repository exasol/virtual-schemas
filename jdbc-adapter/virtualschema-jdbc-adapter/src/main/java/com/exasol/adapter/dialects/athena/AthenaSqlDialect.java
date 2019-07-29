package com.exasol.adapter.dialects.athena;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.RemoteMetadataReader;

/**
 * This class implements the SQL dialect of Amazon's AWS Athena.
 *
 * @see <a href="https://aws.amazon.com/athena/">AWS Athena</a>
 */
public class AthenaSqlDialect extends AbstractSqlDialect {
    static final String NAME = "ATHENA";
    private static final Capabilities CAPABILITIES = createCapabilityList();
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
            DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    @Override
    public String getName() {
        return NAME;
    }

    private static Capabilities createCapabilityList() {
        return Capabilities //
                .builder() //
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT) //
                .addLiteral(NULL, BOOL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL) //
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IS_NULL,
                        IS_NOT_NULL) //
                .addScalarFunction(CAST, ABS, CEIL, ACOS, ASIN, ATAN, ATAN2, COS, COSH, DEGREES, EXP, FLOOR, LN, LOG,
                        MOD, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SQRT, TAN, TANH, TRUNC, BIT_AND, BIT_NOT, BIT_OR,
                        BIT_XOR, CHR, CONCAT, LENGTH, LOWER, LPAD, LTRIM, REPLACE, REVERSE, RPAD, RTRIM, SUBSTR, TRIM,
                        UPPER, CURRENT_DATE, CURRENT_TIMESTAMP, DATE_TRUNC, MINUTE, SECOND, DAY, MONTH, WEEK, YEAR,
                        REGEXP_REPLACE, HASH_MD5, HASH_SHA1) //
                .addAggregateFunction(COUNT, COUNT_STAR, SUM, MIN, MAX, AVG, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE,
                        VAR_POP, VAR_SAMP, APPROXIMATE_COUNT_DISTINCT) //
                .build();
    }

    /**
     * Create a new instance of the {@link AthenaSqlDialect}.
     *
     * @param connection JDBC connection to the Athena service
     * @param properties user-defined adapter properties
     */
    public AthenaSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
    }

    /**
     * Get the type of support Athena has for catalogs.
     * <p>
     * While Athena itself does not use catalogs, the JDBC driver simulates a single catalog for better compatibility
     * with standard products like BI tools.
     * <p>
     *
     * @return always {@link com.exasol.adapter.dialects.SqlDialect.StructureElementSupport#SINGLE}
     *
     * @see <a href=
     *      "https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC_2.0.7/docs/Simba+Athena+JDBC+Driver+Install+and+Configuration+Guide.pdf">
     *      Simba Athena JDBC Driver Install and Configuration Guide, section "Catalog and Schema Support"</a>
     */
    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.SINGLE;
    }

    /**
     * Get the type of support Athena has for schemas.
     * <p>
     * Athena knows schemas as a mirror of databases.
     *
     * @return always {@link com.exasol.adapter.dialects.SqlDialect.StructureElementSupport#MULTIPLE}
     *
     * @see <a href= "https://docs.aws.amazon.com/athena/latest/ug/show-databases.html">SHOW DATABASES documentation</a>
     */
    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
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
    public String applyQuote(final String identifier) {
        if (identifier.startsWith("_")) {
            return quoteWithBackticks(identifier);
        } else {
            return quoteWithDoubleQuotes(identifier);
        }
    }

    private String quoteWithBackticks(final String identifier) {
        final StringBuilder builder = new StringBuilder("`");
        builder.append(identifier);
        builder.append("`");
        return builder.toString();
    }

    private String quoteWithDoubleQuotes(final String identifier) {
        final StringBuilder builder = new StringBuilder("\"");
        builder.append(identifier);
        builder.append("\"");
        return builder.toString();
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new AthenaMetadataReader(this.connection, this.properties);
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, this.remoteMetadataReader, this.connection);
    }

    @Override
    protected List<String> getSupportedProperties() {
        return SUPPORTED_PROPERTIES;
    }
}