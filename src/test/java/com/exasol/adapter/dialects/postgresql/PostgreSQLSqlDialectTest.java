package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionFactory;

@ExtendWith(MockitoExtension.class)
class PostgreSQLSqlDialectTest {
    private PostgreSQLSqlDialect dialect;
    @Mock
    private ConnectionFactory connectionFactoryMock;

    @BeforeEach
    void beforeEach() {
        this.dialect = new PostgreSQLSqlDialect(this.connectionFactoryMock, AdapterProperties.emptyProperties());
    }

    @Test
    void testCreateRemoteMetadataReader() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createRemoteMetadataReader"),
                instanceOf(PostgreSQLMetadataReader.class));
    }

    @Test
    void testCreateQueryRewriter() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createQueryRewriter"),
                instanceOf(BaseQueryRewriter.class));
    }

    @Test
    void testGetCapabilities() {
        final Capabilities capabilities = this.dialect.getCapabilities();
        assertAll(
                () -> assertThat(capabilities.getMainCapabilities(),
                        containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                                AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                                AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT,
                                LIMIT_WITH_OFFSET, JOIN, JOIN_TYPE_INNER, JOIN_TYPE_LEFT_OUTER, JOIN_TYPE_RIGHT_OUTER,
                                JOIN_TYPE_FULL_OUTER, JOIN_CONDITION_EQUI)),
                () -> assertThat(capabilities.getLiteralCapabilities(),
                        containsInAnyOrder(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING)),
                () -> assertThat(capabilities.getPredicateCapabilities(),
                        containsInAnyOrder(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN,
                                REGEXP_LIKE, IN_CONSTLIST, IS_NULL, IS_NOT_NULL)),
                () -> assertThat(capabilities.getAggregateFunctionCapabilities(),
                        containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG,
                                AVG_DISTINCT, MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP,
                                STDDEV_POP_DISTINCT, STDDEV_SAMP, STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT,
                                VAR_POP, VAR_POP_DISTINCT, VAR_SAMP, VAR_SAMP_DISTINCT, GROUP_CONCAT)), //
                () -> assertThat(capabilities.getScalarFunctionCapabilities(),
                        containsInAnyOrder(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS,
                                COSH, COT, DEGREES, DIV, EXP, FLOOR, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS,
                                RAND, ROUND, SIGN, SIN, SINH, SQRT, TAN, TANH, TRUNC, ASCII, BIT_LENGTH, CHR, CONCAT,
                                INSTR, LENGTH, LOWER, LPAD, LTRIM, OCTET_LENGTH, REGEXP_REPLACE, REPEAT, REPLACE,
                                REVERSE, RIGHT, RPAD, RTRIM, SUBSTR, TRANSLATE, TRIM, UNICODE, UNICODECHR, UPPER,
                                ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS, ADD_SECONDS, ADD_WEEKS, ADD_YEARS,
                                SECONDS_BETWEEN, MINUTES_BETWEEN, HOURS_BETWEEN, DAYS_BETWEEN, MONTHS_BETWEEN,
                                YEARS_BETWEEN, MINUTE, SECOND, DAY, WEEK, MONTH, YEAR, CURRENT_DATE, CURRENT_TIMESTAMP,
                                DATE_TRUNC, EXTRACT, LOCALTIMESTAMP, POSIX_TIME, TO_CHAR, CASE, HASH_MD5)));
    }

    @Test
    void testApplyQuoteOnUpperCase() {
        assertEquals("\"abc\"", this.dialect.applyQuote("ABC"));
    }

    @Test
    void testApplyQuoteOnMixedCase() {
        assertEquals("\"abcde\"", this.dialect.applyQuote("AbCde"));
    }

    @Test
    void testMetadataReaderClass() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createRemoteMetadataReader"),
                instanceOf(PostgreSQLMetadataReader.class));
    }

    @Test
    void testPostgreSQLIdentifierMappingConsistency() throws PropertyValidationException {
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, new AdapterProperties(Map.of( //
                SQL_DIALECT_PROPERTY, "postgresql", //
                CONNECTION_NAME_PROPERTY, "MY_CONN", //
                "POSTGRESQL_IDENTIFIER_MAPPING", "CONVERT_TO_UPPER")));
        sqlDialect.validateProperties();
    }

    @Test
    void testPostgreSQLIdentifierMappingInvalidPropertyValueThrowsException() {
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, new AdapterProperties(Map.of( //
                SQL_DIALECT_PROPERTY, "POSTGRESQL", //
                CONNECTION_NAME_PROPERTY, "MY_CONN", //
                "POSTGRESQL_IDENTIFIER_MAPPING", "CONVERT")));
        assertThrows(PropertyValidationException.class, sqlDialect::validateProperties);
    }

    @Test
    void testIgnoreErrorsConsistency() {
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, new AdapterProperties(Map.of( //
                SQL_DIALECT_PROPERTY, "postgresql", //
                CONNECTION_NAME_PROPERTY, "MY_CONN", //
                "IGNORE_ERRORS", "ORACLE_ERROR")));
        assertThrows(PropertyValidationException.class, sqlDialect::validateProperties);
    }

    @Test
    void testValidateCatalogProperty() throws PropertyValidationException {
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, new AdapterProperties(Map.of( //
                SQL_DIALECT_PROPERTY, "POSTGRESQL", //
                CONNECTION_NAME_PROPERTY, "MY_CONN", //
                CATALOG_NAME_PROPERTY, "MY_CATALOG")));
        sqlDialect.validateProperties();
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, new AdapterProperties(Map.of( //
                SQL_DIALECT_PROPERTY, "POSTGRESQL", //
                CONNECTION_NAME_PROPERTY, "MY_CONN", //
                SCHEMA_NAME_PROPERTY, "MY_SCHEMA")));
        sqlDialect.validateProperties();
    }
}