package com.exasol.adapter.dialects.teradata;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

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
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.PropertyValidationException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.jdbc.ConnectionFactory;

@ExtendWith(MockitoExtension.class)
class TeradataSqlDialectTest {
    private SqlDialect dialect;
    @Mock
    private ConnectionFactory connnectionFactoryMock;

    @BeforeEach
    void beforeEach() {
        this.dialect = new TeradataSqlDialect(this.connnectionFactoryMock, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetCapabilities() {
        final Capabilities capabilities = this.dialect.getCapabilities();
        assertAll(() -> assertThat(capabilities.getMainCapabilities(),
                containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                        AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                        AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT)), //
                () -> assertThat(capabilities.getLiteralCapabilities(),
                        containsInAnyOrder(NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING,
                                INTERVAL)),
                () -> assertThat(capabilities.getPredicateCapabilities(),
                        containsInAnyOrder(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE,
                                REGEXP_LIKE, BETWEEN, IN_CONSTLIST, IS_NULL, IS_NOT_NULL)),
                () -> assertThat(capabilities.getAggregateFunctionCapabilities(),
                        containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG,
                                AVG_DISTINCT, MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV_POP, STDDEV_SAMP, VAR_POP,
                                VAR_SAMP)),
                () -> assertThat(capabilities.getScalarFunctionCapabilities(),
                        containsInAnyOrder(CEIL, DIV, FLOOR, SIGN, ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN,
                                ATAN, ATAN2, COS, COSH, COT, DEGREES, EXP, GREATEST, LEAST, LN, LOG, MOD, POWER,
                                RADIANS, SIN, SINH, SQRT, TAN, TANH, ASCII, CHR, INSTR, LENGTH, LOCATE, LOWER, LPAD,
                                LTRIM, REGEXP_INSTR, REGEXP_REPLACE, REGEXP_SUBSTR, REPEAT, REPLACE, REVERSE, RPAD,
                                RTRIM, SOUNDEX, SUBSTR, TRANSLATE, TRIM, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES,
                                ADD_MONTHS, ADD_SECONDS, ADD_WEEKS, ADD_YEARS, CURRENT_DATE, CURRENT_TIMESTAMP,
                                NULLIFZERO, ZEROIFNULL, TRUNC, ROUND)));
    }

    @Test
    void testMetadataReaderClass(@Mock final Connection connectionMock) throws SQLException {
        when(this.connnectionFactoryMock.getConnection()).thenReturn(connectionMock);
        assertThat(getMethodReturnViaReflection(this.dialect, "createRemoteMetadataReader"),
                instanceOf(TeradataMetadataReader.class));
    }

    @Test
    void testValidateCatalogProperty() {
        final AdapterProperties adapterProperties = new AdapterProperties(Map.of( //
                SQL_DIALECT_PROPERTY, "TERADATA", //
                CONNECTION_NAME_PROPERTY, "MY_CONN", //
                CATALOG_NAME_PROPERTY, "MY_CATALOG"));
        final SqlDialect sqlDialect = new TeradataSqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString(
                "The dialect TERADATA does not support CATALOG_NAME property. Please, do not set the \"CATALOG_NAME\" property."));
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        final AdapterProperties adapterProperties = new AdapterProperties(Map.of( //
                SQL_DIALECT_PROPERTY, "TERADATA", //
                CONNECTION_NAME_PROPERTY, "MY_CONN", //
                SCHEMA_NAME_PROPERTY, "MY_SCHEMA"));
        final SqlDialect sqlDialect = new TeradataSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
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
        assertThat(this.dialect.getDefaultNullSorting(), equalTo(SqlDialect.NullSorting.NULLS_SORTED_HIGH));
    }

    @Test
    void testGetSqlGenerationVisitor() {
        assertThat(this.dialect.getSqlGenerationVisitor(null),
                CoreMatchers.instanceOf(TeradataSqlGenerationVisitor.class));
    }

    @Test
    void testSupportsJdbcCatalogs() {
        assertThat(this.dialect.supportsJdbcCatalogs(), equalTo(SqlDialect.StructureElementSupport.NONE));
    }

    @Test
    void testSupportsJdbcSchemas() {
        assertThat(this.dialect.supportsJdbcSchemas(), equalTo(SqlDialect.StructureElementSupport.MULTIPLE));
    }
}