package com.exasol.adapter.dialects.db2;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.PropertyValidationException;

@ExtendWith(MockitoExtension.class)
class DB2SqlDialectTest {
    private SqlDialect dialect;
    @Mock
    private Connection connectionMock;

    @BeforeEach
    void beforeEach() {
        this.dialect = new DB2SqlDialect(this.connectionMock, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetCapabilities() {
        final Capabilities capabilities = this.dialect.getCapabilities();
        assertAll(() -> assertThat(capabilities.getMainCapabilities(),
                containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                        AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                        AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT)),
                () -> assertThat(capabilities.getLiteralCapabilities(),
                        containsInAnyOrder(NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING,
                                INTERVAL)),
                () -> assertThat(capabilities.getPredicateCapabilities(),
                        containsInAnyOrder(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN,
                                IN_CONSTLIST, IS_NULL, IS_NOT_NULL)),
                () -> assertThat(capabilities.getAggregateFunctionCapabilities(),
                        containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, GROUP_CONCAT, GROUP_CONCAT_SEPARATOR,
                                GROUP_CONCAT_ORDER_BY, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT, MEDIAN,
                                FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VARIANCE_DISTINCT,
                                VAR_POP, VAR_SAMP)),
                () -> assertThat(capabilities.getScalarFunctionCapabilities(),
                        containsInAnyOrder(CEIL, DIV, FLOOR, SIGN, ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN,
                                ATAN, ATAN2, COS, COSH, COT, DEGREES, EXP, GREATEST, LEAST, LN, LOG, MOD, POWER,
                                RADIANS, SIN, SINH, SQRT, TAN, TANH, ASCII, CHR, INSTR, LENGTH, LOCATE, LOWER, LPAD,
                                LTRIM, REPEAT, REPLACE, RIGHT, RPAD, RTRIM, SOUNDEX, SUBSTR, TRANSLATE, TRIM, UPPER,
                                ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS, ADD_SECONDS, ADD_WEEKS, ADD_YEARS,
                                CURRENT_DATE, CURRENT_TIMESTAMP, LOCALTIMESTAMP, SYSDATE, SYSTIMESTAMP, CAST, TO_CHAR,
                                TO_DATE, TO_NUMBER, TO_TIMESTAMP, CASE, CURRENT_SCHEMA, CURRENT_USER, NULLIFZERO,
                                ZEROIFNULL)));
    }

    @Test
    void testMetadataReaderClass() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createRemoteMetadataReader"),
                instanceOf(DB2MetadataReader.class));
    }

    @Test
    void testValidateDialectNameProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "ORACLE");
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DB2SqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(), containsString(
                "The dialect DB2 cannot have the name ORACLE. You specified the wrong dialect name or created the wrong dialect class."));
    }

    @Test
    void testValidateCatalogProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "DB2");
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        properties.put(CATALOG_NAME_PROPERTY, "MY_CATALOG");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DB2SqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(), containsString(
                "The dialect DB2 does not support catalogs. Please, do not set the CATALOG_NAME property."));
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "DB2");
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        properties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DB2SqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }
}