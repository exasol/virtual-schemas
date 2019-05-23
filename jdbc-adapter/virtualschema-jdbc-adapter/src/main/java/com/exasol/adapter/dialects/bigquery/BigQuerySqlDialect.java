package com.exasol.adapter.dialects.bigquery;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.*;
import java.util.*;

import com.exasol.adapter.*;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;

/**
 * This class implement the SQL dialect of Google's Big Query
 *
 * @see <a href="https://aws.amazon.com/athena/">AWS Athena</a>
 */
public class BigQuerySqlDialect extends AbstractSqlDialect {
    private static final String NAME = "BIG QUERY";
    private static final Capabilities CAPABILITIES = createCapabilityList();
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY,
            PASSWORD_PROPERTY, CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY,
            EXCLUDED_CAPABILITIES_PROPERTY, DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    /**
     * Get the Big Query dialect name
     *
     * @return always "BIG QUERY"
     */
    public static String getPublicName() {
        return NAME;
    }

    /**
     * Create a new instance of the {@link BigQuerySqlDialect}
     *
     * @param connection JDBC connection to the Big Query
     * @param properties user-defined adapter properties
     */
    public BigQuerySqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    private static Capabilities createCapabilityList() {
        return Capabilities //
                .builder() //
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                        AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET) //
                .addLiteral(NULL, BOOL, DATE, TIMESTAMP, EXACTNUMERIC, STRING) //
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE,
                        BETWEEN, IN_CONSTLIST, IS_NULL, IS_NOT_NULL) //
                .addScalarFunction(ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COSH, DEGREES, DIV, EXP,
                        FLOOR, GREATEST, LEAST, LN, LOG, MOD, POWER, RAND, ROUND, SIGN, SIN, SINH,
                        SQRT, TAN, TANH, TRUNC, COLOGNE_PHONETIC, CONCAT, INSERT, INSTR, LENGTH,
                        LOWER, LPAD, LTRIM, REGEXP_REPLACE, REPEAT, REPLACE, REVERSE, RIGHT, RPAD,
                        RTRIM, SOUNDEX, SPACE, SUBSTR, TRIM, UPPER, CURRENT_DATE, CURRENT_TIMESTAMP,
                        DATE_TRUNC, DAY, EXTRACT, MINUTE, MONTH, SECOND, WEEK, YEAR, ST_X, ST_Y,
                        ST_LENGTH, ST_NUMPOINTS, ST_AREA, ST_BOUNDARY, ST_CENTROID, ST_CONTAINS,
                        ST_DIFFERENCE, ST_DIMENSION, ST_DISJOINT, ST_DISTANCE, ST_EQUALS,
                        ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY, ST_TOUCHES, ST_UNION, ST_WITHIN,
                        CAST, TO_TIMESTAMP, BIT_AND, BIT_OR, BIT_XOR, CASE, HASH_MD5) // TODO md5
                                                                                      // translation
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN,
                        MAX, AVG, AVG_DISTINCT, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT,
                        STDDEV_POP, STDDEV_POP_DISTINCT, STDDEV_SAMP, STDDEV_SAMP_DISTINCT,
                        VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT, VAR_SAMP,
                        VAR_SAMP_DISTINCT, GROUP_CONCAT, APPROXIMATE_COUNT_DISTINCT) // TODO
                                                                                     // APPROXIMATE_COUNT_DISTINCT
                                                                                     // translation
                .build();
    }

    @Override
    protected List<String> getSupportedProperties() {
        return SUPPORTED_PROPERTIES;
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
    }

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    public String applyQuote(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(SqlGenerationContext context) {
        return true;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(SqlGenerationContext context) {
        return true;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    public String getStringLiteral(String value) {
        final StringBuilder builder = new StringBuilder("'");
        builder.append(value.replaceAll("'", "''"));
        builder.append("'");
        return builder.toString();
    }
}