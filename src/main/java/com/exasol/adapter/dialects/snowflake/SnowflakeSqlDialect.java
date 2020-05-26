package com.exasol.adapter.dialects.snowflake;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.adapter.dialects.SqlDialect.StructureElementSupport.MULTIPLE;
import static com.exasol.adapter.dialects.SqlDialect.StructureElementSupport.SINGLE;

import java.sql.SQLException;
import java.util.Set;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;

/**
 * This class implements the SQL dialect of Snowflake.
 *
 * @see <a href="https://snowflake.com/">SNOWFLAKE</a>
 */
public class SnowflakeSqlDialect extends AbstractSqlDialect {
    static final String NAME = "SNOWFLAKE";
    private static final Capabilities CAPABILITIES = createCapabilityList();

    private static Capabilities createCapabilityList() {
        return Capabilities.builder()
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT)
                .addLiteral(NULL, BOOL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL)
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IS_NULL,
                        IS_NOT_NULL)
                .addScalarFunction(CAST, ABS, CEIL, ACOS, ASIN, ATAN, ATAN2, COS, COSH, DEGREES, EXP, FLOOR, LN, LOG,
                        MOD, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SQRT, TAN, TANH, TRUNC, BIT_AND, BIT_NOT, BIT_OR,
                        BIT_XOR, CHR, CONCAT, LENGTH, LOWER, LPAD, LTRIM, REPLACE, REVERSE, RPAD, RTRIM, SUBSTR, TRIM,
                        UPPER, CURRENT_DATE, CURRENT_TIMESTAMP, DATE_TRUNC, MINUTE, SECOND, DAY, MONTH, WEEK, YEAR,
                        REGEXP_REPLACE, HASH_MD5, HASH_SHA1)
                .addAggregateFunction(COUNT, COUNT_STAR, SUM, MIN, MAX, AVG, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE,
                        VAR_POP, VAR_SAMP, APPROXIMATE_COUNT_DISTINCT)
                .build();
    }

    /**
     * Create a new instance of the {@link SnowflakeSqlDialect}.
     *
     * @param connectionFactory factory for the JDBC connection to the remote data source
     * @param properties        user-defined adapter properties
     */
    public SnowflakeSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties, Set.of(CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY));
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
    }

    /**
     * Get the type of support Snowflake has for catalogs.
     * <p>
     * ANSI uses the term “catalog” to refer to databases. To maintain compatibility with the standard, the Snowflake
     * Information Schema topics use “catalog” in place of “database” where applicable. For all intents and purposes,
     * the terms are conceptually equivalent and interchangeable.
     * <p>
     *
     * @return always {@link com.exasol.adapter.dialects.SqlDialect.StructureElementSupport#SINGLE}
     *
     * @see <a href= "https://docs.snowflake.com/en/sql-reference/info-schema.html"> Information Schema
     *      "Catalog and Schema Support"</a>
     */

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return SINGLE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return MULTIPLE;
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public String applyQuote(final String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_START;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new SnowflakeMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to create Snowflake remote metadata reader.", exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, this.createRemoteMetadataReader(), this.connectionFactory);
    }

    @Override
    public String getStringLiteral(final String value) {
        final StringBuilder builder = new StringBuilder("'");
        builder.append(value.replace("'", "''"));
        builder.append("'");
        return builder.toString();
    }

}
