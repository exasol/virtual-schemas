package com.exasol.adapter.dialects.sybase;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
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
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.PropertyValidationException;

class SybaseSqlDialectTest {
    private SqlDialect dialect;

    @BeforeEach
    void beforeEach() {
        this.dialect = new SybaseSqlDialect(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetCapabilities() {
        final Capabilities capabilities = this.dialect.getCapabilities();
        assertAll(() -> assertThat(capabilities.getMainCapabilities(),
                containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                        AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                        AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT)), //
                () -> assertThat(capabilities.getLiteralCapabilities(),
                        containsInAnyOrder(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING,
                                INTERVAL)), //
                () -> assertThat(capabilities.getPredicateCapabilities(),
                        containsInAnyOrder(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN,
                                REGEXP_LIKE, IN_CONSTLIST, IS_NULL, IS_NOT_NULL)),
                () -> assertThat(capabilities.getAggregateFunctionCapabilities(),
                        containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG,
                                AVG_DISTINCT, MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP,
                                STDDEV_POP_DISTINCT, VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT)), //
                () -> assertThat(capabilities.getScalarFunctionCapabilities(),
                        containsInAnyOrder(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COT,
                                DEGREES, EXP, FLOOR, LOG, MOD, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SQRT, TAN, TRUNC,
                                ASCII, CHR, CONCAT, INSTR, LENGTH, LOCATE, LOWER, LPAD, LTRIM, REPEAT, REPLACE, REVERSE,
                                RIGHT, RPAD, RTRIM, SOUNDEX, SPACE, SUBSTR, TRIM, UNICODE, UPPER, ADD_DAYS, ADD_HOURS,
                                ADD_MINUTES, ADD_MONTHS, ADD_SECONDS, ADD_WEEKS, ADD_YEARS, SECONDS_BETWEEN,
                                MINUTES_BETWEEN, HOURS_BETWEEN, DAYS_BETWEEN, MONTHS_BETWEEN, YEARS_BETWEEN, DAY, MONTH,
                                YEAR, SYSDATE, SYSTIMESTAMP, CURRENT_DATE, CURRENT_TIMESTAMP, ST_X, ST_Y, ST_ENDPOINT,
                                ST_ISCLOSED, ST_ISRING, ST_LENGTH, ST_NUMPOINTS, ST_POINTN, ST_STARTPOINT, ST_AREA,
                                ST_EXTERIORRING, ST_INTERIORRINGN, ST_NUMINTERIORRINGS, ST_GEOMETRYN, ST_NUMGEOMETRIES,
                                ST_BOUNDARY, ST_BUFFER, ST_CENTROID, ST_CONTAINS, ST_CONVEXHULL, ST_CROSSES,
                                ST_DIFFERENCE, ST_DIMENSION, ST_DISJOINT, ST_DISTANCE, ST_ENVELOPE, ST_EQUALS,
                                ST_GEOMETRYTYPE, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY, ST_ISSIMPLE, ST_OVERLAPS,
                                ST_SYMDIFFERENCE, ST_TOUCHES, ST_UNION, ST_WITHIN, BIT_AND, BIT_NOT, BIT_OR, BIT_XOR,
                                CASE, HASH_MD5, HASH_SHA, HASH_SHA1, NULLIFZERO, ZEROIFNULL)));
    }

    @Test
    void testValidateCatalogProperty() throws PropertyValidationException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "SYBASE");
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        properties.put(CATALOG_NAME_PROPERTY, "MY_CATALOG");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new SybaseSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testValidateDialectNameProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "ORACLE");
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new SybaseSqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(), containsString(
                "The dialect SYBASE cannot have the name ORACLE. You specified the wrong dialect name or created the wrong dialect class."));
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "SYBASE");
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        properties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new SybaseSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }
}