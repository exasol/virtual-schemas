package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.dialects.SqlDialectContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import static org.mockito.Mockito.*;

import java.sql.ResultSet;
import java.sql.SQLException;

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
    public void applyQuoteOnUpperCase() {
        PostgreSQLSqlDialect postgresDialect = new PostgreSQLSqlDialect(sqlDialectContext);
        assertEquals("\"abc\"", postgresDialect.applyQuote("ABC"));
    }

    @Test
    public void applyQuoteOnMixedCase() {
        PostgreSQLSqlDialect postgresDialect = new PostgreSQLSqlDialect(sqlDialectContext);
        assertEquals("\"abcde\"", postgresDialect.applyQuote("AbCde"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void mapTableWithUpperCaseCharacters() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("uPPer");
        PostgreSQLSqlDialect postgresDialect = new PostgreSQLSqlDialect(sqlDialectContext);
        postgresDialect.mapTable(resultSet);
    }

    @Test
    public void mapTableWithLowerCaseCharacters() throws SQLException {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("lower");
        PostgreSQLSqlDialect postgresDialect = new PostgreSQLSqlDialect(sqlDialectContext);
        postgresDialect.mapTable(resultSet);
    }
}