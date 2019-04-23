package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.PropertyValidationException;
import com.exasol.adapter.dialects.SqlDialect;

class PostgreSQLSqlDialectTest {
    private PostgreSQLSqlDialect dialect;
    private Map<String, String> rawProperties;

    @BeforeEach
    void beforeEach() {
        this.rawProperties = new HashMap<>();
        this.dialect = new PostgreSQLSqlDialect(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetCapabilities() {
        final Capabilities capabilities = this.dialect.getCapabilities();
        assertAll(
                () -> assertThat(capabilities.getMainCapabilities(),
                        containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                                AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                                AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT,
                                LIMIT_WITH_OFFSET)), //
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
    void checkPostgreSQLIdentifierMappingConsistencyThrowsException() {
        setMandatoryProperties("ORACLE");
        this.rawProperties.put("POSTGRESQL_IDENTIFIER_MAPPING", "CONVERT_TO_UPPER");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, adapterProperties);
        assertThrows(PropertyValidationException.class, sqlDialect::validateProperties);
    }

    @Test
    void checkPostgreSQLIdentifierMappingConsistency() throws PropertyValidationException {
        setMandatoryProperties("POSTGRESQL");
        this.rawProperties.put("POSTGRESQL_IDENTIFIER_MAPPING", "CONVERT_TO_UPPER");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void checkPostgreSQLIdentifierMappingInvalidPropertyValueThrowsException() {
        setMandatoryProperties("POSTGRESQL");
        this.rawProperties.put("POSTGRESQL_IDENTIFIER_MAPPING", "CONVERT");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, adapterProperties);
        assertThrows(PropertyValidationException.class, sqlDialect::validateProperties);
    }

    @Test
    void checkIgnoreErrorsConsistency() {
        this.rawProperties.put("IGNORE_ERRORS", "ORACLE_ERROR");
        this.rawProperties.put("SQL_DIALECT", "postgresql");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, adapterProperties);
        assertThrows(PropertyValidationException.class, sqlDialect::validateProperties);
    }

    @Test
    void testGetDefaultPostgreSQLIdentifierMapping() {
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, adapterProperties);
        final String val = this.dialect.getPostgreSQLIdentifierMapping();
        assertEquals("CONVERT_TO_UPPER", val);
    }

    @Test
    void testGetPreserveCasePostgreSQLIdentifierMapping() {
        this.rawProperties.put("POSTGRESQL_IDENTIFIER_MAPPING", "PRESERVE_ORIGINAL_CASE");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final PostgreSQLSqlDialect sqlDialect = new PostgreSQLSqlDialect(null, adapterProperties);
        final String val = sqlDialect.getPostgreSQLIdentifierMapping();
        assertEquals("PRESERVE_ORIGINAL_CASE", val);
    }

    @Test
    void testGetConverToUpperPostgreSQLIdentifierMapping() {
        this.rawProperties.put("POSTGRESQL_IDENTIFIER_MAPPING", "CONVERT_TO_UPPER");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final PostgreSQLSqlDialect sqlDialect = new PostgreSQLSqlDialect(null, adapterProperties);
        final String val = sqlDialect.getPostgreSQLIdentifierMapping();
        assertEquals("CONVERT_TO_UPPER", val);
    }

    @Test
    void testValidateCatalogProperty() throws PropertyValidationException {
        setMandatoryProperties("POSTGRESQL");
        this.rawProperties.put(CATALOG_NAME_PROPERTY, "MY_CATALOG");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testValidateDialectNameProperty() {
        setMandatoryProperties("ORACLE");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(), containsString(
                "The dialect POSTGRESQL cannot have the name ORACLE. You specified the wrong dialect name or created the wrong dialect class."));
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        setMandatoryProperties("POSTGRESQL");
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new PostgreSQLSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    private void setMandatoryProperties(final String sqlDialectProperty) {
        this.rawProperties.put(AdapterProperties.SQL_DIALECT_PROPERTY, sqlDialectProperty);
        this.rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, "MY_CONN");
    }
}