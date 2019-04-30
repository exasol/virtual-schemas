package com.exasol.adapter.dialects.impala;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
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

class ImpalaSqlDialectTest {
    private SqlDialect dialect;
    private Map<String, String> rawProperties;

    @BeforeEach
    void beforeEach() {
        this.dialect = new ImpalaSqlDialect(null, AdapterProperties.emptyProperties());
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
                        containsInAnyOrder(NULL, DOUBLE, EXACTNUMERIC, STRING, BOOL)),
                () -> assertThat(capabilities.getPredicateCapabilities(),
                        containsInAnyOrder(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN,
                                IN_CONSTLIST, IS_NULL, IS_NOT_NULL)),
                () -> assertThat(capabilities.getAggregateFunctionCapabilities(), containsInAnyOrder(COUNT, COUNT_STAR,
                        COUNT_DISTINCT, GROUP_CONCAT, GROUP_CONCAT_SEPARATOR, SUM, SUM_DISTINCT, MIN, MAX, AVG)));
    }

    @Test
    void testValidateCatalogProperty() throws PropertyValidationException {
        setMandatoryProperties("IMPALA");
        this.rawProperties.put(CATALOG_NAME_PROPERTY, "MY_CATALOG");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ImpalaSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testValidateDialectNameProperty() {
        setMandatoryProperties("ORACLE");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ImpalaSqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(), containsString(
                "The dialect IMPALA cannot have the name ORACLE. You specified the wrong dialect name or created the wrong dialect class."));
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        setMandatoryProperties("IMPALA");
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ImpalaSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    private void setMandatoryProperties(final String sqlDialectProperty) {
        this.rawProperties.put(AdapterProperties.SQL_DIALECT_PROPERTY, sqlDialectProperty);
        this.rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, "MY_CONN");
    }
}