package com.exasol.adapter.dialects.redshift;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.HashMap;
import java.util.Map;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.PropertyValidationException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialect.NullSorting;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.sql.ScalarFunction;

@ExtendWith(MockitoExtension.class)
class RedshiftSqlDialectTest {
    private SqlDialect dialect;
    private Map<String, String> rawProperties;

    @BeforeEach
    void beforeEach(@Mock final ConnectionFactory connectionFactoryMock) {
        this.dialect = new RedshiftSqlDialect(connectionFactoryMock, AdapterProperties.emptyProperties());
        this.rawProperties = new HashMap<>();
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
                        containsInAnyOrder(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING,
                                INTERVAL)), //
                () -> assertThat(capabilities.getPredicateCapabilities(),
                        containsInAnyOrder(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN,
                                REGEXP_LIKE, IN_CONSTLIST, IS_NULL, IS_NOT_NULL)),
                () -> assertThat(capabilities.getAggregateFunctionCapabilities(),
                        containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG,
                                AVG_DISTINCT, MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP,
                                STDDEV_POP_DISTINCT, STDDEV_SAMP, STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT,
                                VAR_POP, VAR_POP_DISTINCT, VAR_SAMP, VAR_SAMP_DISTINCT, GROUP_CONCAT)), //
                () -> assertThat(capabilities.getScalarFunctionCapabilities(),
                        containsInAnyOrder(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COT,
                                DEGREES, DIV, EXP, FLOOR, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, ROUND, SIGN,
                                SIN, SINH, SQRT, TAN, TANH, TRUNC, ASCII, CHR, CONCAT, LOCATE, INSTR, LENGTH, LOWER,
                                LPAD, LTRIM, REGEXP_INSTR, REGEXP_REPLACE, REGEXP_SUBSTR, REPEAT, REPLACE, REVERSE,
                                RIGHT, RPAD, RTRIM, SUBSTR, TRANSLATE, TRIM, UPPER, BIT_AND, BIT_OR, ADD_MONTHS,
                                MONTHS_BETWEEN, CONVERT_TZ, SYSDATE, YEAR, CURRENT_DATE, CURRENT_TIMESTAMP, EXTRACT,
                                CAST, TO_NUMBER, TO_TIMESTAMP, TO_DATE, HASH_SHA1, HASH_MD5, CURRENT_SCHEMA,
                                CURRENT_USER)));
    }

    @Test
    void testMetadataReaderClass() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createRemoteMetadataReader"),
                instanceOf(RedshiftMetadataReader.class));
    }

    @Test
    void testValidateCatalogProperty() throws PropertyValidationException {
        setMandatoryProperties();
        this.rawProperties.put(CATALOG_NAME_PROPERTY, "MY_CATALOG");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new RedshiftSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        setMandatoryProperties();
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new RedshiftSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    private void setMandatoryProperties() {
        this.rawProperties.put(AdapterProperties.SQL_DIALECT_PROPERTY, "REDSHIFT");
        this.rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, "MY_CONN");
    }

    @Test
    void testGetScalarFunctionAliases() {
        final Map<ScalarFunction, String> scalarFunctionAliases = this.dialect.getScalarFunctionAliases();
        assertThat(scalarFunctionAliases, aMapWithSize(5));
    }

    @Test
    void testGetAggregateFunctionAliases() {
        assertThat(this.dialect.getAggregateFunctionAliases(), aMapWithSize(0));
    }

    @CsvSource({ "ABC, \"ABC\"", //
            "AbCde, \"AbCde\"", //
            "\"tableName, \"\"\"tableName\"" //
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
    void testRequiresCatalogQualifiedTableNames() {
        assertThat(this.dialect.requiresCatalogQualifiedTableNames(null), equalTo(false));
    }

    @Test
    void testRequiresSchemaQualifiedTableNames() {
        assertThat(this.dialect.requiresSchemaQualifiedTableNames(null), equalTo(true));
    }

    @Test
    void testGetDefaultNullSorting() {
        assertThat(this.dialect.getDefaultNullSorting(), equalTo(NullSorting.NULLS_SORTED_AT_END));
    }

    @Test
    void testGetSqlGenerationVisitor() {
        assertThat(this.dialect.getSqlGenerationVisitor(null),
                CoreMatchers.instanceOf(RedshiftSqlGenerationVisitor.class));
    }
}