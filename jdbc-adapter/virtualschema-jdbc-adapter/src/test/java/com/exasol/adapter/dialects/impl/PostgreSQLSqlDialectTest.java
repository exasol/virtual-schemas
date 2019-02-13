package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.dialects.SqlDialectContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class PostgreSQLSqlDialectTest {
    @Mock
    SqlDialectContext sqlDialectContext;

    @Before
    public void setUp() throws SQLException {

    }

    @Test
    public void testApplyQuoteOnUpperCase() {
        PostgreSQLSqlDialect postgresDialect = new PostgreSQLSqlDialect(sqlDialectContext);
        assertEquals("\"abc\"", postgresDialect.applyQuote("ABC"));
    }

    @Test
    public void testApplyQuoteOnMixedCase() {
        PostgreSQLSqlDialect postgresDialect = new PostgreSQLSqlDialect(sqlDialectContext);
        assertEquals("\"abcde\"", postgresDialect.applyQuote("AbCde"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapTableWithUpperCaseCharactersAndNoErrorIgnoredThrowsException() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("uPPer");
        PostgreSQLSqlDialect postgresDialect = new PostgreSQLSqlDialect(sqlDialectContext);
        postgresDialect.mapTable(resultSet, Collections.emptyList());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapTableWithRussianUpperCaseCharactersAndNoErrorIgnoredThrowsException() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("аППер");
        PostgreSQLSqlDialect postgresDialect = new PostgreSQLSqlDialect(sqlDialectContext);
        postgresDialect.mapTable(resultSet, Collections.emptyList());
    }

    @Test
    public void testMapTableWithLowerCaseCharacters() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("lower");
        PostgreSQLSqlDialect postgresDialect = new PostgreSQLSqlDialect(sqlDialectContext);
        postgresDialect.mapTable(resultSet, Collections.emptyList());
    }

    @Test
    public void testMapTableWithIgnoreUppercaseCharactersError() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("Upper");
        List<String> ignoreList = new ArrayList<>();
        ignoreList.add("Dummy_Error");
        ignoreList.add("POSTGRESQL_UPPERCASE_TABLES");
        PostgreSQLSqlDialect postgresDialect = new PostgreSQLSqlDialect(sqlDialectContext);
        postgresDialect.mapTable(resultSet, ignoreList);
    }
}