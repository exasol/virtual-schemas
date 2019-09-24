package com.exasol.adapter.dialects.mysql;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.Connection;
import java.util.List;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.RemoteMetadataReader;

/**
 * This class implements the SQL dialect of MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/">MySQL developer documentation</a>
 */
public class MySqlSqlDialect extends AbstractSqlDialect {
    static final String NAME = "MYSQL";
    private static final Capabilities CAPABILITIES = createCapabilityList();
    private static final List<String> SUPPORTED_PROPERTIES = List.of(SQL_DIALECT_PROPERTY, CONNECTION_NAME_PROPERTY,
            CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY, CATALOG_NAME_PROPERTY,
            TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY, DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    /**
     * Create a new instance of the {@link MySqlSqlDialect}.
     *
     * @param connection JDBC connection to the Athena service
     * @param properties user-defined adapter properties
     */
    public MySqlSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    private static Capabilities createCapabilityList() {
        return Capabilities //
                .builder() //
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET) //
                .addLiteral(NULL, BOOL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL) //
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IS_NULL,
                        IS_NOT_NULL) //
                .addScalarFunction(ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COT, DEGREES, DIV, EXP, FLOOR, GREATEST,
                        LEAST, LN, LOG, MOD, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SQRT, TAN, ASCII, BIT_LENGTH,
                        CONCAT, INSERT, INSTR, LENGTH, LOCATE, LOWER, LPAD, LTRIM, OCTET_LENGTH, REGEXP_INSTR,
                        REGEXP_REPLACE, REGEXP_SUBSTR, REPEAT, REPLACE, REVERSE, RIGHT, RPAD, RTRIM, SOUNDEX, SPACE,
                        SUBSTR, TRIM, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS, ADD_SECONDS, ADD_WEEKS,
                        ADD_YEARS, CONVERT_TZ, CURRENT_DATE, CURRENT_TIMESTAMP, EXTRACT, LOCALTIMESTAMP, MINUTE, MONTH,
                        SECOND, SYSDATE, SYSTIMESTAMP, WEEK, YEAR, ST_X, ST_Y, ST_ENDPOINT, ST_ISCLOSED, ST_LENGTH,
                        ST_NUMPOINTS, ST_POINTN, ST_STARTPOINT, ST_AREA, ST_EXTERIORRING, ST_INTERIORRINGN,
                        ST_NUMINTERIORRINGS, ST_GEOMETRYN, ST_NUMGEOMETRIES, ST_BUFFER, ST_CENTROID, ST_CONTAINS,
                        ST_CONVEXHULL, ST_CROSSES, ST_DIFFERENCE, ST_DIMENSION, ST_DISJOINT, ST_DISTANCE, ST_ENVELOPE,
                        ST_EQUALS, ST_GEOMETRYTYPE, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY, ST_ISSIMPLE,
                        ST_OVERLAPS, ST_SYMDIFFERENCE, ST_TOUCHES, ST_TRANSFORM, ST_UNION, ST_WITHIN, CAST, BIT_AND,
                        BIT_OR, BIT_XOR, CASE, CURRENT_USER) //
                .addAggregateFunction(COUNT, SUM, MIN, MAX, AVG, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP,
                        VAR_SAMP) //
                .build();
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
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.NONE;
    }

    /**
     * @see <a href=http://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sqlmode_ansi_quotes><a/>
     */
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
        return false;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new MySqlMetadataReader(this.connection, this.properties);
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
