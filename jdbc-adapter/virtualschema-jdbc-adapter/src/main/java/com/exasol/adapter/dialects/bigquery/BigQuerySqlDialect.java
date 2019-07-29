package com.exasol.adapter.dialects.bigquery;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.Connection;
import java.util.*;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * This class implements the SQL dialect of Google's Big Query.
 *
 * @see <a href="https://cloud.google.com/bigquery/">BigQuery</a>
 */
public class BigQuerySqlDialect extends AbstractSqlDialect {
    private static final String NAME = "BIGQUERY";
    private static final Capabilities CAPABILITIES = createCapabilityList();
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
            DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Create a new instance of the {@link BigQuerySqlDialect}.
     *
     * @param connection JDBC connection to the Big Query service
     * @param properties user-defined adapter properties
     */
    public BigQuerySqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new BigQueryMetadataReader(this.connection, this.properties);

    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BigQueryQueryRewriter(this, this.remoteMetadataReader, this.connection);
    }

    private static Capabilities createCapabilityList() {
        return Capabilities //
                .builder() //
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_EXPRESSION,
                        LIMIT, LIMIT_WITH_OFFSET) //
                .addLiteral(NULL, BOOL, DATE, TIMESTAMP, EXACTNUMERIC, STRING) //
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IN_CONSTLIST,
                        IS_NULL, IS_NOT_NULL) //
                .addScalarFunction(ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COSH, DEGREES, DIV, EXP, FLOOR, GREATEST,
                        LEAST, LN, LOG, MOD, POWER, RAND, ROUND, SIGN, SIN, SINH, SQRT, TAN, TANH, TRUNC,
                        COLOGNE_PHONETIC, CONCAT, INSERT, INSTR, LENGTH, LOWER, LPAD, LTRIM, REGEXP_REPLACE, REPEAT,
                        REPLACE, REVERSE, RIGHT, RPAD, RTRIM, SOUNDEX, SPACE, SUBSTR, TRIM, UPPER, CURRENT_DATE,
                        CURRENT_TIMESTAMP, DATE_TRUNC, DAY, EXTRACT, MINUTE, MONTH, SECOND, WEEK, YEAR, ST_X, ST_Y,
                        ST_LENGTH, ST_NUMPOINTS, ST_AREA, ST_BOUNDARY, ST_CENTROID, ST_CONTAINS, ST_DIFFERENCE,
                        ST_DIMENSION, ST_DISJOINT, ST_DISTANCE, ST_EQUALS, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY,
                        ST_TOUCHES, ST_UNION, ST_WITHIN, CAST, TO_TIMESTAMP, BIT_AND, BIT_OR, BIT_XOR, CASE, HASH_MD5) //
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
                        FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT, STDDEV_SAMP,
                        STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT, VAR_SAMP,
                        VAR_SAMP_DISTINCT, GROUP_CONCAT, APPROXIMATE_COUNT_DISTINCT) //
                .build();
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        final Map<AggregateFunction, String> aliases = new EnumMap<>(AggregateFunction.class);
        aliases.put(AggregateFunction.APPROXIMATE_COUNT_DISTINCT, "APPROX_COUNT_DISTINCT");
        return aliases;
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> aliases = new EnumMap<>(ScalarFunction.class);
        aliases.put(ScalarFunction.HASH_MD5, "MD5");
        return aliases;
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
    public String applyQuote(final String identifier) {
        return "`" + identifier + "`";
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
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new BigQuerySqlGenerationVisitor(this, context);
    }
}