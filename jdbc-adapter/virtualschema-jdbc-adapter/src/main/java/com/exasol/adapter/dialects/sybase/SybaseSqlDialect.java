package com.exasol.adapter.dialects.sybase;

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
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * This class implements the Sybase SQL dialect.
 */
public class SybaseSqlDialect extends AbstractSqlDialect {
    static final int MAX_SYBASE_VARCHAR_SIZE = 8000;
    static final int MAX_SYBASE_N_VARCHAR_SIZE = 4000;
    private static final String NAME = "SYBASE";
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
            DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    /**
     * Create a new instance of the {@link SybaseSqlDialect}.
     *
     * @param connection JDBC connection
     * @param properties user-defined adapter properties
     */
    public SybaseSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    /**
     * Get the Sybase dialect name.
     *
     * @return always "SYBASE"
     */
    public static String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        builder.addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING,
                ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT);
        builder.addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN, REGEXP_LIKE,
                IN_CONSTLIST, IS_NULL, IS_NOT_NULL);
        builder.addLiteral(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL);
        builder.addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
                MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT, VARIANCE,
                VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT);
        builder.addScalarFunction(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COT, DEGREES,
                EXP, FLOOR, LOG, MOD, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SQRT, TAN, TRUNC, ASCII, CHR, CONCAT,
                INSTR, LENGTH, LOCATE, LOWER, LPAD, LTRIM, REPEAT, REPLACE, REVERSE, RIGHT, RPAD, RTRIM, SOUNDEX, SPACE,
                SUBSTR, TRIM, UNICODE, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS, ADD_SECONDS, ADD_WEEKS,
                ADD_YEARS, SECONDS_BETWEEN, MINUTES_BETWEEN, HOURS_BETWEEN, DAYS_BETWEEN, MONTHS_BETWEEN, YEARS_BETWEEN,
                DAY, MONTH, YEAR, SYSDATE, SYSTIMESTAMP, CURRENT_DATE, CURRENT_TIMESTAMP, ST_X, ST_Y, ST_ENDPOINT,
                ST_ISCLOSED, ST_ISRING, ST_LENGTH, ST_NUMPOINTS, ST_POINTN, ST_STARTPOINT, ST_AREA, ST_EXTERIORRING,
                ST_INTERIORRINGN, ST_NUMINTERIORRINGS, ST_GEOMETRYN, ST_NUMGEOMETRIES, ST_BOUNDARY, ST_BUFFER,
                ST_CENTROID, ST_CONTAINS, ST_CONVEXHULL, ST_CROSSES, ST_DIFFERENCE, ST_DIMENSION, ST_DISJOINT,
                ST_DISTANCE, ST_ENVELOPE, ST_EQUALS, ST_GEOMETRYTYPE, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY,
                ST_ISSIMPLE, ST_OVERLAPS, ST_SYMDIFFERENCE, ST_TOUCHES, ST_UNION, ST_WITHIN, BIT_AND, BIT_NOT, BIT_OR,
                BIT_XOR, CASE, HASH_MD5, HASH_SHA, HASH_SHA1, NULLIFZERO, ZEROIFNULL);
        return builder.build();
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.ATAN2, "ATN2");
        scalarAliases.put(ScalarFunction.CEIL, "CEILING");
        scalarAliases.put(ScalarFunction.CHR, "CHAR");
        scalarAliases.put(ScalarFunction.LENGTH, "LEN");
        scalarAliases.put(ScalarFunction.LOCATE, "CHARINDEX");
        scalarAliases.put(ScalarFunction.REPEAT, "REPLICATE");
        scalarAliases.put(ScalarFunction.SUBSTR, "SUBSTRING");
        scalarAliases.put(ScalarFunction.NULLIFZERO, "NULLIF");
        return scalarAliases;
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        final Map<AggregateFunction, String> aggregationAliases = new EnumMap<>(AggregateFunction.class);
        aggregationAliases.put(AggregateFunction.STDDEV, "STDEV");
        aggregationAliases.put(AggregateFunction.STDDEV_POP, "STDEVP");
        aggregationAliases.put(AggregateFunction.VARIANCE, "VAR");
        aggregationAliases.put(AggregateFunction.VAR_POP, "VARP");
        return aggregationAliases;
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
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new SybaseSqlGenerationVisitor(this, context);
    }

    @Override
    public String applyQuote(final String identifier) {
        return "[" + identifier + "]";
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
        return NullSorting.NULLS_SORTED_LOW;
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
        return new BaseRemoteMetadataReader(this.connection, this.properties);
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, this.remoteMetadataReader, this.connection);
    }
}
