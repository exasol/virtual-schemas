package com.exasol.adapter.dialects.bigquery;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ST_INTERSECTION;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ST_UNION;
import static com.exasol.adapter.dialects.bigquery.BigQueryProperties.BIGQUERY_ENABLE_IMPORT_PROPERTY;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

@ExtendWith(MockitoExtension.class)
class BigQuerySqlDialectTest {
    private BigQuerySqlDialect dialect;
    @Mock
    private ConnectionFactory connectionFactoryMock;

    @BeforeEach
    void beforeEach() {
        this.dialect = new BigQuerySqlDialect(this.connectionFactoryMock, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetName() {
        assertThat(this.dialect.getName(), equalTo("BIGQUERY"));
    }

    @Test
    void testGetMainCapabilities() {
        assertThat(this.dialect.getCapabilities().getMainCapabilities(),
                containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                        AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING,
                        ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET));
    }

    @Test
    void testGetLiteralCapabilitiesLiteral() {
        assertThat(this.dialect.getCapabilities().getLiteralCapabilities(),
                containsInAnyOrder(NULL, BOOL, DATE, TIMESTAMP, EXACTNUMERIC, STRING));
    }

    @Test
    void testGetPredicateCapabilities() {
        assertThat(this.dialect.getCapabilities().getPredicateCapabilities(), containsInAnyOrder(AND, OR, NOT, EQUAL,
                NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IN_CONSTLIST, IS_NULL, IS_NOT_NULL));
    }

    @Test
    void testGetScalarFunctionCapabilities() {
        assertThat(this.dialect.getCapabilities().getScalarFunctionCapabilities(),
                containsInAnyOrder(ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COSH, DEGREES, DIV, EXP, FLOOR, GREATEST,
                        LEAST, LN, LOG, MOD, POWER, RAND, ROUND, SIGN, SIN, SINH, SQRT, TAN, TANH, TRUNC,
                        COLOGNE_PHONETIC, CONCAT, INSERT, INSTR, LENGTH, LOWER, LPAD, LTRIM, REGEXP_REPLACE, REPEAT,
                        REPLACE, REVERSE, RIGHT, RPAD, RTRIM, SOUNDEX, SPACE, SUBSTR, TRIM, UPPER, CURRENT_DATE,
                        CURRENT_TIMESTAMP, DATE_TRUNC, DAY, EXTRACT, MINUTE, MONTH, SECOND, WEEK, YEAR, ST_X, ST_Y,
                        ST_LENGTH, ST_NUMPOINTS, ST_AREA, ST_BOUNDARY, ST_CENTROID, ST_CONTAINS, ST_DIFFERENCE,
                        ST_DIMENSION, ST_DISJOINT, ST_DISTANCE, ST_EQUALS, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY,
                        ST_TOUCHES, ST_UNION, ST_WITHIN, CAST, TO_TIMESTAMP, BIT_AND, BIT_OR, BIT_XOR, CASE, HASH_MD5));
    }

    @Test
    void testGetAggregateFunctionCapabilities() {
        assertThat(this.dialect.getCapabilities().getAggregateFunctionCapabilities(),
                containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
                        FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT, STDDEV_SAMP,
                        STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT, VAR_SAMP,
                        VAR_SAMP_DISTINCT, GROUP_CONCAT, APPROXIMATE_COUNT_DISTINCT));
    }

    @Test
    void testSupportsJdbcCatalogs() {
        assertThat(this.dialect.supportsJdbcCatalogs(), equalTo(SqlDialect.StructureElementSupport.MULTIPLE));
    }

    @Test
    void testSupportsJdbcSchemas() {
        assertThat(this.dialect.supportsJdbcSchemas(), equalTo(SqlDialect.StructureElementSupport.MULTIPLE));
    }

    @Test
    void testGetDefaultNullSorting() {
        assertThat(this.dialect.getDefaultNullSorting(), equalTo(SqlDialect.NullSorting.NULLS_SORTED_AT_END));
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
                containsInAnyOrder(SQL_DIALECT_PROPERTY, CONNECTION_NAME_PROPERTY, CATALOG_NAME_PROPERTY,
                        SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
                        DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY, BIGQUERY_ENABLE_IMPORT_PROPERTY));
    }

    @CsvSource({ "5Customers, `5Customers`", //
            "_dataField1, `_dataField1`", //
            "tableName, `tableName`", //
            "\" table123 `, `\" table123 \\``" //
    })
    @ParameterizedTest
    void testApplyQuote(final String unquoted, final String quoted) {
        assertThat(this.dialect.applyQuote(unquoted), equalTo(quoted));
    }

    @ValueSource(strings = { "ab:'ab'", "a'b:'a\\'b'", "a''b:'a\\'\\'b'", "'ab':'\\'ab\\''", "a\\b:'a\\\\b'",
            "a\\\\b:'a\\\\\\\\b'", "a\\'b:'a\\\\\\'b'" })
    @ParameterizedTest
    void testGetLiteralString(final String definition) {
        assertThat(this.dialect.getStringLiteral(definition.substring(0, definition.indexOf(':'))),
                equalTo(definition.substring(definition.indexOf(':') + 1)));
    }

    @Test
    void testGetLiteralStringNull() {
        assertThat(this.dialect.getStringLiteral(null), CoreMatchers.equalTo("NULL"));
    }

    @Test
    void testGetAggregateFunctionAliases() {
        assertThat(this.dialect.getAggregateFunctionAliases().get(AggregateFunction.APPROXIMATE_COUNT_DISTINCT),
                equalTo("APPROX_COUNT_DISTINCT"));
    }

    @Test
    void testGetScalarFunctionAliases() {
        assertThat(this.dialect.getScalarFunctionAliases().get(ScalarFunction.HASH_MD5), equalTo("MD5"));
    }

    @Test
    void testCreateQueryRewriterBigQueryRewriter(@Mock final Connection connectionMock) throws SQLException {
        Mockito.when(this.connectionFactoryMock.getConnection()).thenReturn(connectionMock);
        assertThat(this.dialect.createQueryRewriter(), instanceOf(BigQueryQueryRewriter.class));
    }

    @Test
    void testCreateQueryRewriterBaseQueryRewriter(@Mock final ConnectionFactory connectionFactory) {
        final AdapterProperties adapterProperties = new AdapterProperties(
                Map.of(BIGQUERY_ENABLE_IMPORT_PROPERTY, "TRUE"));
        final BigQuerySqlDialect dialect = new BigQuerySqlDialect(connectionFactory, adapterProperties);
        assertThat(dialect.createQueryRewriter(), instanceOf(ImportIntoQueryRewriter.class));
    }

    @Test
    void testValidateProperties() {
        final AdapterProperties adapterProperties = new AdapterProperties(Map.of( //
                BIGQUERY_ENABLE_IMPORT_PROPERTY, "WRONG VALUE", //
                CONNECTION_NAME_PROPERTY, "CONNECTION_NAME_PROPERTY"));
        final BigQuerySqlDialect dialect = new BigQuerySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                dialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString("The value 'WRONG VALUE' for the property BIGQUERY_ENABLE_IMPORT is invalid. "
                        + "It has to be either 'true' or 'false' (case insensitive)"));
    }
}
