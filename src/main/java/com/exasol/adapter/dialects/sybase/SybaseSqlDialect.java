package com.exasol.adapter.dialects.sybase;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ST_INTERSECTION;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ST_UNION;

import java.sql.SQLException;
import java.util.*;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.*;

/**
 * This class implements the Sybase SQL dialect.
 */
public class SybaseSqlDialect extends AbstractSqlDialect {
    static final String NAME = "SYBASE";
    static final int MAX_SYBASE_VARCHAR_SIZE = 8000;
    static final int MAX_SYBASE_N_VARCHAR_SIZE = 4000;
    private static final Capabilities CAPABILITIES = createCapabilityList();

    private static Capabilities createCapabilityList() {
        return Capabilities.builder()
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT)
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN, REGEXP_LIKE,
                        IN_CONSTLIST, IS_NULL, IS_NOT_NULL)
                .addLiteral(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL)
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
                        MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT,
                        VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT)
                .addScalarFunction(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COT,
                        DEGREES, EXP, FLOOR, LOG, MOD, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SQRT, TAN, TRUNC, ASCII,
                        CHR, CONCAT, INSTR, LENGTH, LOCATE, LOWER, LPAD, LTRIM, REPEAT, REPLACE, REVERSE, RIGHT, RPAD,
                        RTRIM, SOUNDEX, SPACE, SUBSTR, TRIM, UNICODE, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES,
                        ADD_MONTHS, ADD_SECONDS, ADD_WEEKS, ADD_YEARS, SECONDS_BETWEEN, MINUTES_BETWEEN, HOURS_BETWEEN,
                        DAYS_BETWEEN, MONTHS_BETWEEN, YEARS_BETWEEN, DAY, MONTH, YEAR, SYSDATE, SYSTIMESTAMP,
                        CURRENT_DATE, CURRENT_TIMESTAMP, ST_X, ST_Y, ST_ENDPOINT, ST_ISCLOSED, ST_ISRING, ST_LENGTH,
                        ST_NUMPOINTS, ST_POINTN, ST_STARTPOINT, ST_AREA, ST_EXTERIORRING, ST_INTERIORRINGN,
                        ST_NUMINTERIORRINGS, ST_GEOMETRYN, ST_NUMGEOMETRIES, ST_BOUNDARY, ST_BUFFER, ST_CENTROID,
                        ST_CONTAINS, ST_CONVEXHULL, ST_CROSSES, ST_DIFFERENCE, ST_DIMENSION, ST_DISJOINT, ST_DISTANCE,
                        ST_ENVELOPE, ST_EQUALS, ST_GEOMETRYTYPE, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY,
                        ST_ISSIMPLE, ST_OVERLAPS, ST_SYMDIFFERENCE, ST_TOUCHES, ST_UNION, ST_WITHIN, BIT_AND, BIT_NOT,
                        BIT_OR, BIT_XOR, CASE, HASH_MD5, HASH_SHA1, NULLIFZERO, ZEROIFNULL)
                .build();
    }

    /**
     * Create a new instance of the {@link SybaseSqlDialect}.
     *
     * @param connectionFactory factory for the JDBC connection to the remote data source
     * @param properties        user-defined adapter properties
     */
    public SybaseSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
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
    public SqlNodeVisitor<String> getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new SybaseSqlGenerationVisitor(this, context);
    }

    @Override
    // http://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.infocenter.dc36271.1600/doc/html/san1393050529478.html
    public String applyQuote(final String identifier) {
        return SybaseIdentifier.of(identifier).quote();
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
    // http://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.infocenter.dc01031.0400/doc/html/asc1252677176370.html
    public String getStringLiteral(final String value) {
        if (value == null) {
            return "NULL";
        } else if (value.contains("\n") || value.contains("\r") || value.contains("\\")) {
            throw new IllegalArgumentException("Sybase string literal contains illegal characters: \\n or \\r or \\.");
        } else {
            return "'" + value.replace("'", "''") + "'";
        }
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new BaseRemoteMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(
                    "Unable to create Sybase remote metadata reader. Caused by: " + exception.getMessage(), exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new ImportIntoQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
    }
}