package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.metadata.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.adapter.metadata.DataType.ExaDataType.VARCHAR;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PostgreSQLSqlDialectTest {
    @Mock
    private SqlDialectContext sqlDialectContext;
    private PostgreSQLSqlDialect postgresDialect;
    private SqlDialect dialect;

    @BeforeEach
    void beforeEach() {
        this.dialect = new PostgreSQLSqlDialect(null);
        this.postgresDialect = new PostgreSQLSqlDialect(this.sqlDialectContext);
    }

    @Test
    void testGetCapabilities() {
        final Capabilities capabilities = this.dialect.getCapabilities();
        assertAll(() -> assertThat(capabilities.getMainCapabilities(),
              containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                    AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                    AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT,
                    LIMIT_WITH_OFFSET)),//
              () -> assertThat(capabilities.getLiteralCapabilities(),
                    containsInAnyOrder(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING)),
              () -> assertThat(capabilities.getPredicateCapabilities(),
                    containsInAnyOrder(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN,
                          REGEXP_LIKE, IN_CONSTLIST, IS_NULL, IS_NOT_NULL)),
              () -> assertThat(capabilities.getAggregateFunctionCapabilities(),
                    containsInAnyOrder(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG,
                          AVG_DISTINCT, MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP,
                          STDDEV_POP_DISTINCT, STDDEV_SAMP, STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT, VAR_POP,
                          VAR_POP_DISTINCT, VAR_SAMP, VAR_SAMP_DISTINCT, GROUP_CONCAT)), //
              () -> assertThat(capabilities.getScalarFunctionCapabilities(),
                    containsInAnyOrder(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COSH,
                          COT, DEGREES, DIV, EXP, FLOOR, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, RAND, ROUND,
                          SIGN, SIN, SINH, SQRT, TAN, TANH, TRUNC, ASCII, BIT_LENGTH, CHR, CONCAT, INSTR, LENGTH, LOWER,
                          LPAD, LTRIM, OCTET_LENGTH, REGEXP_REPLACE, REPEAT, REPLACE, REVERSE, RIGHT, RPAD, RTRIM,
                          SUBSTR, TRANSLATE, TRIM, UNICODE, UNICODECHR, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES,
                          ADD_MONTHS, ADD_SECONDS, ADD_WEEKS, ADD_YEARS, SECONDS_BETWEEN, MINUTES_BETWEEN,
                          HOURS_BETWEEN, DAYS_BETWEEN, MONTHS_BETWEEN, YEARS_BETWEEN, MINUTE, SECOND, DAY, WEEK, MONTH,
                          YEAR, CURRENT_DATE, CURRENT_TIMESTAMP, DATE_TRUNC, EXTRACT, LOCALTIMESTAMP, POSIX_TIME,
                          TO_CHAR, CASE, HASH_MD5)));
    }

    @Test
    void testApplyQuoteOnUpperCase() {
        assertEquals("\"abc\"", this.postgresDialect.applyQuote("ABC"));
    }

    @Test
    void testApplyQuoteOnMixedCase() {
        assertEquals("\"abcde\"", this.postgresDialect.applyQuote("AbCde"));
    }

    @Test
    void testMapTableWithUpperCaseCharactersAndNoErrorIgnoredThrowsException() throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("uPPer");
        assertThrows(IllegalArgumentException.class,
              () -> this.postgresDialect.mapTable(resultSet, Collections.emptyList()));
    }

    @Test
    void testMapTableWithRussianUpperCaseCharactersAndNoErrorIgnoredThrowsException() throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("аППер");
        assertThrows(IllegalArgumentException.class,
              () -> this.postgresDialect.mapTable(resultSet, Collections.emptyList()));
    }

    @Test
    void testMapTableWithLowerCaseCharacters() throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("lower");
        this.postgresDialect.mapTable(resultSet, Collections.emptyList());
    }

    @Test
    void testMapTableWithIgnoreUppercaseCharactersError() throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("Upper");
        final List<String> ignoreList = new ArrayList<>();
        ignoreList.add("Dummy_Error");
        ignoreList.add("POSTGRESQL_UPPERCASE_TABLES");
        this.postgresDialect.mapTable(resultSet, ignoreList);
    }

    @Test
    void testDialectSpecificMapJdbcTypeDistinct() throws SQLException {
        final JdbcTypeDescription jdbcTypeDescription = mock(JdbcTypeDescription.class);
        when(jdbcTypeDescription.getJdbcType()).thenReturn(Types.DISTINCT);
        assertVarcharDataType(jdbcTypeDescription);
    }

    @Test
    void testDialectSpecificMapJdbcTypeSQLXML() throws SQLException {
        final JdbcTypeDescription jdbcTypeDescription = mock(JdbcTypeDescription.class);
        when(jdbcTypeDescription.getJdbcType()).thenReturn(Types.SQLXML);
        assertVarcharDataType(jdbcTypeDescription);
    }

    private void assertVarcharDataType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        final DataType dataType = this.postgresDialect.dialectSpecificMapJdbcType(jdbcTypeDescription);
        assertEquals(DataType.ExaCharset.UTF8, dataType.getCharset());
        assertEquals(2000000, dataType.getSize());
        assertEquals(VARCHAR, dataType.getExaDataType());
    }
}