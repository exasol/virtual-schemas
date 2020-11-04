package com.exasol.adapter.dialects.hive;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.PropertyValidationException;
import com.exasol.adapter.dialects.SqlDialect;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class HiveSqlDialectTest {
    private SqlDialect dialect;
    private Map<String, String> rawProperties;

    @BeforeEach
    void beforeEach() {
        this.dialect = new HiveSqlDialect(null, AdapterProperties.emptyProperties());
        this.rawProperties = new HashMap<>();
    }

    @Test
    void testGetCapabilities() {
        final Capabilities capabilities = this.dialect.getCapabilities();
        assertAll(
                () -> assertThat(capabilities.getMainCapabilities(),
                        containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                                AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_HAVING, ORDER_BY_COLUMN,
                                ORDER_BY_EXPRESSION, LIMIT, JOIN, JOIN_TYPE_INNER, JOIN_TYPE_LEFT_OUTER,
                                JOIN_TYPE_RIGHT_OUTER, JOIN_TYPE_FULL_OUTER, JOIN_CONDITION_EQUI)), //
                () -> assertThat(capabilities.getLiteralCapabilities(),
                        containsInAnyOrder(NULL, BOOL, DATE, TIMESTAMP, DOUBLE, EXACTNUMERIC, STRING)),
                () -> assertThat(capabilities.getPredicateCapabilities(),
                        containsInAnyOrder(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN,
                                IN_CONSTLIST, IS_NULL, IS_NOT_NULL)),
                () -> assertThat(capabilities.getAggregateFunctionCapabilities(),
                        containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG,
                                AVG_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT, STDDEV_SAMP, STDDEV_SAMP_DISTINCT,
                                VAR_POP, VAR_POP_DISTINCT, VAR_SAMP, VAR_SAMP_DISTINCT)),
                () -> assertThat(capabilities.getScalarFunctionCapabilities(),
                        containsInAnyOrder(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, CEIL, COS, DEGREES,
                                DIV, EXP, FLOOR, LN, LOG, MOD, POWER, RADIANS, SIGN, SIN, SQRT, TAN, ASCII, CONCAT,
                                LENGTH, LOWER, LPAD, REPEAT, REVERSE, RPAD, SOUNDEX, SPACE, SUBSTR, TRANSLATE, UPPER,
                                ADD_DAYS, ADD_MONTHS, CURRENT_DATE, CURRENT_TIMESTAMP, DATE_TRUNC, DAY, DAYS_BETWEEN,
                                MINUTE, MONTH, MONTHS_BETWEEN, SECOND, WEEK, CAST, BIT_AND, BIT_OR, BIT_XOR,
                                CURRENT_USER)));
    }

    @Test
    void testValidateCatalogProperty() throws PropertyValidationException {
        setMandatoryProperties("HIVE");
        this.rawProperties.put(CATALOG_NAME_PROPERTY, "MY_CATALOG");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new HiveSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        setMandatoryProperties("HIVE");
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new HiveSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @CsvSource({ "tableName, `tableName`", //
            "`tableName, ```tableName`" //
    })
    @ParameterizedTest
    void testApplyQuote(final String unquoted, final String quoted) {
        assertThat(this.dialect.applyQuote(unquoted), equalTo(quoted));
    }

    private void setMandatoryProperties(final String sqlDialectProperty) {
        this.rawProperties.put(AdapterProperties.SQL_DIALECT_PROPERTY, sqlDialectProperty);
        this.rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, "MY_CONN");
    }
}