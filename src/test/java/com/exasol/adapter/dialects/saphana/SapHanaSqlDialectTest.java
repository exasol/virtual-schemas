package com.exasol.adapter.dialects.saphana;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ST_INTERSECTION;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ST_UNION;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.sql.ScalarFunction;

@ExtendWith(MockitoExtension.class)
class SapHanaSqlDialectTest {
    private SapHanaSqlDialect dialect;
    @Mock
    private ConnectionFactory connectionFactoryMock;

    @BeforeEach
    void beforeEach() {
        this.dialect = new SapHanaSqlDialect(this.connectionFactoryMock, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetName() {
        assertThat(this.dialect.getName(), equalTo("SAPHANA"));
    }

    @Test
    void testGetMainCapabilities() {
        assertThat(this.dialect.getCapabilities().getMainCapabilities(),
                containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                        AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                        AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT,
                        LIMIT_WITH_OFFSET));
    }

    @Test
    void testGetLiteralCapabilitiesLiteral() {
        assertThat(this.dialect.getCapabilities().getLiteralCapabilities(),
                containsInAnyOrder(NULL, BOOL, DATE, TIMESTAMP, DOUBLE, EXACTNUMERIC, STRING));
    }

    @Test
    void testGetPredicateCapabilities() {
        assertThat(this.dialect.getCapabilities().getPredicateCapabilities(), containsInAnyOrder(AND, OR, NOT, EQUAL,
                NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IS_NULL, IS_NOT_NULL));
    }

    @Test
    void testGetScalarFunctionCapabilities() {
        assertThat(this.dialect.getCapabilities().getScalarFunctionCapabilities(), containsInAnyOrder(ABS, ACOS, ASIN,
                ATAN, ATAN2, CEIL, COS, COSH, COT, FLOOR, GREATEST, LEAST, LN, LOG, MOD, POWER, RAND, ROUND, SIGN, SIN,
                SINH, SQRT, TAN, TANH, ASCII, CONCAT, LENGTH, LOCATE, LOWER, LPAD, LTRIM, REGEXP_REPLACE, REGEXP_SUBSTR,
                RIGHT, RPAD, RTRIM, SOUNDEX, SUBSTR, TRIM, UNICODE, UPPER, ADD_DAYS, ADD_MONTHS, ADD_SECONDS, ADD_YEARS,
                CURRENT_DATE, CURRENT_TIMESTAMP, DAYS_BETWEEN, EXTRACT, MINUTE, MONTH, MONTHS_BETWEEN, SECOND,
                SECONDS_BETWEEN, WEEK, YEAR, YEARS_BETWEEN, ST_X, ST_Y, ST_ENDPOINT, ST_ISCLOSED, ST_ISRING, ST_LENGTH,
                ST_NUMPOINTS, ST_POINTN, ST_STARTPOINT, ST_AREA, ST_EXTERIORRING, ST_INTERIORRINGN, ST_NUMINTERIORRINGS,
                ST_GEOMETRYN, ST_NUMGEOMETRIES, ST_BOUNDARY, ST_BUFFER, ST_CENTROID, ST_CONTAINS, ST_CONVEXHULL,
                ST_CROSSES, ST_DIFFERENCE, ST_DIMENSION, ST_DISJOINT, ST_DISTANCE, ST_ENVELOPE, ST_EQUALS,
                ST_GEOMETRYTYPE, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY, ST_ISSIMPLE, ST_OVERLAPS, ST_SYMDIFFERENCE,
                ST_TOUCHES, ST_TRANSFORM, ST_UNION, ST_WITHIN, CAST, TO_DATE, TO_TIMESTAMP, BIT_AND, BIT_NOT, BIT_OR,
                BIT_SET, BIT_XOR, CURRENT_SCHEMA, CURRENT_USER, HASH_MD5, HASH_SHA256));
    }

    @Test
    void testGetAggregateFunctionCapabilities() {
        assertThat(this.dialect.getCapabilities().getAggregateFunctionCapabilities(),
                containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, MIN, MAX, AVG, MEDIAN, FIRST_VALUE,
                        LAST_VALUE, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP));
    }

    @Test
    void testSupportsJdbcCatalogs() {
        assertThat(this.dialect.supportsJdbcCatalogs(), Matchers.equalTo(SqlDialect.StructureElementSupport.SINGLE));
    }

    @Test
    void testSupportsJdbcSchemas() {
        assertThat(this.dialect.supportsJdbcSchemas(), Matchers.equalTo(SqlDialect.StructureElementSupport.MULTIPLE));
    }

    @Test
    void testRequiresCatalogQualifiedTableNames() {
        assertThat(this.dialect.requiresCatalogQualifiedTableNames(null), Matchers.equalTo(false));
    }

    @Test
    void testRequiresSchemaQualifiedTableNames() {
        assertThat(this.dialect.requiresSchemaQualifiedTableNames(null), Matchers.equalTo(true));
    }

    @Test
    void testGetDefaultNullSorting() {
        assertThat(this.dialect.getDefaultNullSorting(),
                Matchers.equalTo(SqlDialect.NullSorting.NULLS_SORTED_AT_START));
    }

    @CsvSource({ "tableName, \"tableName\"", //
            "\"tableName, \"\"\"tableName\"" //
    })
    @ParameterizedTest
    void testApplyQuote(final String unquoted, final String quoted) {
        assertThat(this.dialect.applyQuote(unquoted), Matchers.equalTo(quoted));
    }

    @ValueSource(strings = { "ab:'ab'", "a'b:'a''b'", "a''b:'a''''b'", "'ab':'''ab'''" })
    @ParameterizedTest
    void testGetLiteralString(final String definition) {
        assertThat(this.dialect.getStringLiteral(definition.substring(0, definition.indexOf(':'))),
                Matchers.equalTo(definition.substring(definition.indexOf(':') + 1)));
    }

    @Test
    void testMetadataReaderClass(@Mock final Connection connectionMock) throws SQLException {
        when(this.connectionFactoryMock.getConnection()).thenReturn(connectionMock);
        assertThat(getMethodReturnViaReflection(this.dialect, "createRemoteMetadataReader"),
                instanceOf(SapHanaMetadataReader.class));
    }

    @Test
    void testGetSupportedProperties() {
        assertThat(this.dialect.getSupportedProperties(),
                containsInAnyOrder(SQL_DIALECT_PROPERTY, CONNECTION_NAME_PROPERTY, CATALOG_NAME_PROPERTY,
                        SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
                        DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY));
    }

    @Test
    void testGetScalarFunctionAliases() {
        assertAll(
                () -> assertThat(this.dialect.getScalarFunctionAliases().get(ScalarFunction.REGEXP_SUBSTR),
                        Matchers.equalTo("SUBSTRING_REGEXPR")), //
                () -> assertThat(this.dialect.getScalarFunctionAliases().get(ScalarFunction.REGEXP_REPLACE),
                        Matchers.equalTo("REPLACE_REGEXPR")), //
                () -> assertThat(this.dialect.getScalarFunctionAliases().get(ScalarFunction.SUBSTR),
                        Matchers.equalTo("SUBSTRING")), //
                () -> assertThat(this.dialect.getScalarFunctionAliases().get(ScalarFunction.BIT_AND),
                        Matchers.equalTo("BITAND")), //
                () -> assertThat(this.dialect.getScalarFunctionAliases().get(ScalarFunction.BIT_NOT),
                        Matchers.equalTo("BITNOT")), //
                () -> assertThat(this.dialect.getScalarFunctionAliases().get(ScalarFunction.BIT_OR),
                        Matchers.equalTo("BITOR")), //
                () -> assertThat(this.dialect.getScalarFunctionAliases().get(ScalarFunction.BIT_SET),
                        Matchers.equalTo("BITSET")), //
                () -> assertThat(this.dialect.getScalarFunctionAliases().get(ScalarFunction.BIT_XOR),
                        Matchers.equalTo("BITXOR")));
    }
}