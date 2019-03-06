package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.metadata.DataType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.exasol.adapter.metadata.DataType.ExaDataType.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PostgreSQLSqlDialectTest {
    @Mock private SqlDialectContext sqlDialectContext;
    private PostgreSQLSqlDialect postgresDialect;

    @Before
    public void setUp() {
        postgresDialect = new PostgreSQLSqlDialect(sqlDialectContext);
    }

    @Test
    public void testApplyQuoteOnUpperCase() {
        assertEquals("\"abc\"", postgresDialect.applyQuote("ABC"));
    }

    @Test
    public void testApplyQuoteOnMixedCase() {
        assertEquals("\"abcde\"", postgresDialect.applyQuote("AbCde"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapTableWithUpperCaseCharactersAndNoErrorIgnoredThrowsException() throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("uPPer");
        postgresDialect.mapTable(resultSet, Collections.emptyList());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMapTableWithRussianUpperCaseCharactersAndNoErrorIgnoredThrowsException() throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("аППер");
        postgresDialect.mapTable(resultSet, Collections.emptyList());
    }

    @Test
    public void testMapTableWithLowerCaseCharacters() throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("lower");
        postgresDialect.mapTable(resultSet, Collections.emptyList());
    }

    @Test
    public void testMapTableWithIgnoreUppercaseCharactersError() throws SQLException {
        final ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("TABLE_NAME")).thenReturn("Upper");
        final List<String> ignoreList = new ArrayList<>();
        ignoreList.add("Dummy_Error");
        ignoreList.add("POSTGRESQL_UPPERCASE_TABLES");
        postgresDialect.mapTable(resultSet, ignoreList);
    }

    @Test
    public void testDialectSpecificMapJdbcTypeDistinct() throws SQLException {
        final JdbcTypeDescription jdbcTypeDescription = mock(JdbcTypeDescription.class);
        when(jdbcTypeDescription.getJdbcType()).thenReturn(Types.DISTINCT);
        assertVarcharDataType(jdbcTypeDescription);
    }

    @Test
    public void testDialectSpecificMapJdbcTypeSQLXML() throws SQLException {
        final JdbcTypeDescription jdbcTypeDescription = mock(JdbcTypeDescription.class);
        when(jdbcTypeDescription.getJdbcType()).thenReturn(Types.SQLXML);
        assertVarcharDataType(jdbcTypeDescription);
    }

    private void assertVarcharDataType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        final DataType dataType = postgresDialect.dialectSpecificMapJdbcType(jdbcTypeDescription);

        assertEquals(DataType.ExaCharset.UTF8, dataType.getCharset());
        assertEquals(2000000, dataType.getSize());
        assertEquals(VARCHAR, dataType.getExaDataType());
    }
}