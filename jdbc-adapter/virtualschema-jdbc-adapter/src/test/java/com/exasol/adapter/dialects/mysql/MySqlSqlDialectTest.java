package com.exasol.adapter.dialects.mysql;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect.NullSorting;
import com.exasol.adapter.dialects.SqlDialect.StructureElementSupport;

public class MySqlSqlDialectTest {
    private MySqlSqlDialect dialect;

    @BeforeEach
    public void beforeEach() {
        this.dialect = new MySqlSqlDialect(null, AdapterProperties.emptyProperties());
    }


    @Test
    void testGetName() {
        assertThat(this.dialect.getName(), equalTo("MYSQL"));
    }

    @Test
    public void testGetMainCapabilities() {
        assertThat(this.dialect.getCapabilities().getMainCapabilities(),
                containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                        AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                        AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT,
                        LIMIT_WITH_OFFSET));
    }

    @Test
    public void testGetLiteralCapabilitiesLiteral() {
        assertThat(this.dialect.getCapabilities().getLiteralCapabilities(),
                containsInAnyOrder(NULL, BOOL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL));
    }

    @Test
    public void testGetScalarFunctionCapabilities() {
        assertThat(this.dialect.getCapabilities().getScalarFunctionCapabilities(), containsInAnyOrder(ABS, ACOS, ASIN,
                ATAN, ATAN2, CEIL, COS, COT, DEGREES, DIV, EXP, FLOOR, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS,
                RAND, ROUND, SIGN, SIN, SQRT, TAN, ASCII, BIT_LENGTH, CONCAT, INSERT, INSTR, LENGTH, LOCATE, LOWER,
                LPAD, LTRIM, OCTET_LENGTH, REGEXP_INSTR, REGEXP_REPLACE, REGEXP_SUBSTR, REPEAT, REPLACE, REVERSE, RIGHT,
                RPAD, RTRIM, SOUNDEX, SPACE, SUBSTR, TRIM, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS,
                ADD_SECONDS, ADD_WEEKS, ADD_YEARS, CONVERT_TZ, CURRENT_DATE, CURRENT_TIMESTAMP, EXTRACT, LOCALTIMESTAMP,
                MINUTE, MONTH, SECOND, SYSDATE, SYSTIMESTAMP, WEEK, YEAR, ST_X, ST_Y, ST_ENDPOINT, ST_ISCLOSED,
                ST_LENGTH, ST_NUMPOINTS, ST_POINTN, ST_STARTPOINT, ST_AREA, ST_EXTERIORRING, ST_INTERIORRINGN,
                ST_NUMINTERIORRINGS, ST_GEOMETRYN, ST_NUMGEOMETRIES, ST_BUFFER, ST_CENTROID, ST_CONTAINS, ST_CONVEXHULL,
                ST_CROSSES, ST_DIFFERENCE, ST_DIMENSION, ST_DISJOINT, ST_DISTANCE, ST_ENVELOPE, ST_EQUALS,
                ST_GEOMETRYTYPE, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY, ST_ISSIMPLE, ST_OVERLAPS, ST_SYMDIFFERENCE,
                ST_TOUCHES, ST_TRANSFORM, ST_UNION, ST_WITHIN, CAST, BIT_AND, BIT_OR, BIT_XOR, CASE, CURRENT_USER));
    }

    @Test
    public void testGetAggregateFunctionCapabilities() {
        assertThat(this.dialect.getCapabilities().getAggregateFunctionCapabilities(), containsInAnyOrder(COUNT, SUM,
                MIN, MAX, AVG, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP));
    }

    @Test
    public void testSupportsJdbcCatalogs() {
        assertThat(this.dialect.supportsJdbcCatalogs(), equalTo(StructureElementSupport.NONE));
    }

    @Test
    public void testSupportsJdbcSchemas() {
        assertThat(this.dialect.supportsJdbcSchemas(), equalTo(StructureElementSupport.SINGLE));
    }

    @Test
    public void testRequiresCatalogQualifiedTableNames() {
        assertThat(this.dialect.requiresCatalogQualifiedTableNames(null), equalTo(false));
    }

    @Test
    public void testRequiresSchemaQualifiedTableNames() {
        assertThat(this.dialect.requiresSchemaQualifiedTableNames(null), equalTo(false));
    }

    @Test
    public void testGetDefaultNullSorting() {
        assertThat(this.dialect.getDefaultNullSorting(), equalTo(NullSorting.NULLS_SORTED_AT_END));
    }

    @CsvSource({ "tableName, \"tableName\"", "table123, \"table123\"", "_table, `_table`",
            "table_name, \"table_name\"" })
    @ParameterizedTest
    public void testApplyQuote(final String unquoted, final String quoted) {
        assertThat(this.dialect.applyQuote(unquoted), equalTo(quoted));
    }

    @Test
    public void testMetadataReaderClass() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createRemoteMetadataReader"),
                instanceOf(MySqlMetadataReader.class));
    }

    @Test
    public void testGetSupportedProperties() {
        assertThat(this.dialect.getSupportedProperties(),
                containsInAnyOrder(SQL_DIALECT_PROPERTY, CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY,
                        USERNAME_PROPERTY, PASSWORD_PROPERTY, CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY,
                        TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY, DEBUG_ADDRESS_PROPERTY,
                        LOG_LEVEL_PROPERTY));
    }
}
