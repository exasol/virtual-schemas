package com.exasol.adapter.dialects.bigquery;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import com.exasol.adapter.dialects.*;
import org.junit.jupiter.api.*;

import com.exasol.adapter.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

class BigQuerySqlDialectTest {
    private BigQuerySqlDialect dialect;

    @BeforeEach
    void beforeEach() {
        this.dialect = new BigQuerySqlDialect(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetPublicName() {
        assertThat(BigQuerySqlDialect.getPublicName(), equalTo("BIGQUERY"));
    }

    @Test
    void testGetMainCapabilities() {
        assertThat(this.dialect.getCapabilities().getMainCapabilities(),
                containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS,
                        FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN,
                        AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_EXPRESSION, LIMIT,
                        LIMIT_WITH_OFFSET));
    }

    @Test
    void testGetLiteralCapabilitiesLiteral() {
        assertThat(this.dialect.getCapabilities().getLiteralCapabilities(),
                containsInAnyOrder(NULL, BOOL, DATE, TIMESTAMP, EXACTNUMERIC, STRING));
    }

    @Test
    void testGetPredicateCapabilities() {
        assertThat(this.dialect.getCapabilities().getPredicateCapabilities(),
                containsInAnyOrder(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE,
                        REGEXP_LIKE, BETWEEN, IN_CONSTLIST, IS_NULL, IS_NOT_NULL));
    }

    @Test
    void testGetScalarFunctionCapabilities() {
        assertThat(this.dialect.getCapabilities().getScalarFunctionCapabilities(),
                containsInAnyOrder(ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COSH, DEGREES, DIV, EXP,
                        FLOOR, GREATEST, LEAST, LN, LOG, MOD, POWER, RAND, ROUND, SIGN, SIN, SINH,
                        SQRT, TAN, TANH, TRUNC, COLOGNE_PHONETIC, CONCAT, INSERT, INSTR, LENGTH,
                        LOWER, LPAD, LTRIM, REGEXP_REPLACE, REPEAT, REPLACE, REVERSE, RIGHT, RPAD,
                        RTRIM, SOUNDEX, SPACE, SUBSTR, TRIM, UPPER, CURRENT_DATE, CURRENT_TIMESTAMP,
                        DATE_TRUNC, DAY, EXTRACT, MINUTE, MONTH, SECOND, WEEK, YEAR, ST_X, ST_Y,
                        ST_LENGTH, ST_NUMPOINTS, ST_AREA, ST_BOUNDARY, ST_CENTROID, ST_CONTAINS,
                        ST_DIFFERENCE, ST_DIMENSION, ST_DISJOINT, ST_DISTANCE, ST_EQUALS,
                        ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY, ST_TOUCHES, ST_UNION, ST_WITHIN,
                        CAST, TO_TIMESTAMP, BIT_AND, BIT_OR, BIT_XOR, CASE, HASH_MD5));
    }

    @Test
    void testGetAggregateFunctionCapabilities() {
        assertThat(this.dialect.getCapabilities().getAggregateFunctionCapabilities(),
                containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX,
                        AVG, AVG_DISTINCT, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT,
                        STDDEV_POP, STDDEV_POP_DISTINCT, STDDEV_SAMP, STDDEV_SAMP_DISTINCT,
                        VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT, VAR_SAMP,
                        VAR_SAMP_DISTINCT, GROUP_CONCAT, APPROXIMATE_COUNT_DISTINCT));
    }

    @Test
    void testSupportsJdbcCatalogs() {
        assertThat(this.dialect.supportsJdbcCatalogs(),
                equalTo(SqlDialect.StructureElementSupport.MULTIPLE));
    }

    @Test
    void testSupportsJdbcSchemas() {
        assertThat(this.dialect.supportsJdbcSchemas(),
                equalTo(SqlDialect.StructureElementSupport.MULTIPLE));
    }

    @Test
    void testGetDefaultNullSorting() {
        assertThat(this.dialect.getDefaultNullSorting(),
                equalTo(SqlDialect.NullSorting.NULLS_SORTED_AT_END));
    }

    @Test
    void testRequiresCatalogQualifiedTableNames() {
        assertThat(this.dialect.requiresCatalogQualifiedTableNames(null), equalTo(true));
    }

    @Test
    void testRequiresSchemaQualifiedTableNames() {
        assertThat(this.dialect.requiresSchemaQualifiedTableNames(null), equalTo(true));
    }

    @Test
    void testGetSupportedProperties() {
        assertThat(this.dialect.getSupportedProperties(),
                containsInAnyOrder(SQL_DIALECT_PROPERTY, CONNECTION_NAME_PROPERTY,
                        CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
                        CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY,
                        EXCLUDED_CAPABILITIES_PROPERTY, DEBUG_ADDRESS_PROPERTY,
                        LOG_LEVEL_PROPERTY));
    }

    @Test
    void testApplyQuote() {
        assertThat(this.dialect.applyQuote("tableName"), equalTo("`tableName`"));
    }
}
