package com.exasol.adapter.dialects.bigquery;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.sql.SqlStatement;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BigQueryQueryRewriterTest extends AbstractQueryRewriterTest {
    private QueryRewriter queryRewriter;
    @Mock
    private ResultSet mockResultSet;
    @Mock
    private ResultSetMetaData mockResultSetMetaData;
    @Mock
    private Statement mockStatement;
    @Mock
    private ExaMetadata exaMetadata;

    @BeforeEach
    void beforeEach() throws SQLException {
        final Connection connectionMock = this.mockConnection();
        this.statement = mock(SqlStatement.class);
        final SqlDialect dialect = new BigQuerySqlDialect(connectionMock, AdapterProperties.emptyProperties());
        final BaseRemoteMetadataReader metadataReader = new BaseRemoteMetadataReader(connectionMock,
                AdapterProperties.emptyProperties());
        this.queryRewriter = new BigQueryQueryRewriter(dialect, metadataReader, connectionMock);
        when(connectionMock.createStatement()).thenReturn(this.mockStatement);
        when(this.mockResultSet.getMetaData()).thenReturn(this.mockResultSetMetaData);
        when(this.mockStatement.executeQuery(any())).thenReturn(this.mockResultSet);
    }

    @Test
    void testRewriteWithJdbcConnectionEmptyTable() throws AdapterException, SQLException {
        when(this.mockResultSet.next()).thenReturn(false);
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn("1");
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (1) WHERE false"));
    }

    @Test
    void testRewriteWithBigInt() throws AdapterException, SQLException {
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn("bigInt");
        when(this.mockResultSet.getInt("bigInt")).thenReturn(1);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(Types.BIGINT);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(this.mockResultSet.next()).thenReturn(true, false);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (1)"));
    }

    @CsvSource({ "float, 8, 105.0", //
            "float, 8, 99.4", //
            "numeric, 2, 22222.2222", //
            "numeric, 2, 11.5" })
    @ParameterizedTest
    void testRewriteWithFloatingValues(final String columnName, final int type, final double columnValue)
            throws AdapterException, SQLException {
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn(columnName);
        when(this.mockResultSet.getDouble(columnName)).thenReturn(columnValue);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(type);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(this.mockResultSet.next()).thenReturn(true, false);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (" + columnValue + ")"));
    }

    @Test
    void testRewriteWithBoolean() throws AdapterException, SQLException {
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn("boolean");
        when(this.mockResultSet.getBoolean("boolean")).thenReturn(true);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(Types.BOOLEAN);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(this.mockResultSet.next()).thenReturn(true, false);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (true)"));
    }

    @CsvSource({"bytes, -3 ,D7A0FA33B3CDDF0EC29FC989CAB9AB512658F6E9A631FE5008610CBA24A64A5C", //
            "time, 92, 12:10:09.000" })
    @ParameterizedTest
    void testRewriteWithVarcharValues(final String columnName, final int type, final String columnValue)
            throws AdapterException, SQLException {
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn(columnName);
        when(this.mockResultSet.getString(columnName)).thenReturn(columnValue);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(type);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(this.mockResultSet.next()).thenReturn(true, false);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES ('" + columnValue + "')"));
    }

    @CsvSource({ "string, 12, hello, hello", //
          "string, 12, i'm, i''m"})
    @ParameterizedTest
    void testRewriteWithStringValues(final String columnName, final int type, final String columnValue, final String resultValue)
          throws AdapterException, SQLException {
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn(columnName);
        when(this.mockResultSet.getString(columnName)).thenReturn(columnValue);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(type);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(this.mockResultSet.next()).thenReturn(true, false);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
              equalTo("SELECT * FROM VALUES ('" + resultValue + "')"));
    }

    @CsvSource({ "1111-01-01, 01.01.1111", //
            "2019-12-3, 03.12.2019", //
            "2019-5-02, 02.05.2019" //
    })
    @ParameterizedTest
    void testRewriteWithDate(final String valueToConvert, final String expectedValue)
            throws AdapterException, SQLException {
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn("date");
        when(this.mockResultSet.getString("date")).thenReturn(valueToConvert);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(91);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(this.mockResultSet.next()).thenReturn(true, false);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES ('" + expectedValue + "')"));
    }

    @CsvSource({ "1111-01-01 12:10:09.000000, 01.01.1111 12:10:09.000", //
            "1111-1-1 12:10:9.000000, 01.01.1111 12:10:09.000", //
            "1111-1-1 12:10:9, 01.01.1111 12:10:09", //
            "1111-1-1 12:10:9.1, 01.01.1111 12:10:09.100", //
            "1111-1-1 12:10:9.12, 01.01.1111 12:10:09.120", //
            "1111-1-1 12:10:9.123, 01.01.1111 12:10:09.123", //
            "1111-1-1 12:10:9.1234, 01.01.1111 12:10:09.123", //
            "1111-1-1 12:10:9.1239, 01.01.1111 12:10:09.124", //
            "1111-1-1 12:10:9.12345, 01.01.1111 12:10:09.123", //
            "1111-1-1 12:10:9.123666, 01.01.1111 12:10:09.124", //
            "1111-1-1T1:2:30, 01.01.1111 01:02:30" //
    })
    @ParameterizedTest
    void testRewriteWithDatetime(final String valueToConvert, final String expectedValue)
            throws AdapterException, SQLException {
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn("timestamp");
        when(this.mockResultSet.getString("timestamp")).thenReturn(valueToConvert);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(93);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(this.mockResultSet.next()).thenReturn(true, false);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES ('" + expectedValue + "')"));
    }

    @Test
    void testRewriteWithJdbcConnectionWithThreeRows() throws AdapterException, SQLException {
        when(this.mockResultSet.next()).thenReturn(true, true, true, false);
        when(this.mockResultSet.getInt(any())).thenReturn(1, 2, 3);
        when(this.mockResultSet.getString(any())).thenReturn("foo", "bar", "cat");
        when(this.mockResultSet.getBoolean(any())).thenReturn(true, false, true);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(Types.BIGINT);
        when(this.mockResultSetMetaData.getColumnType(2)).thenReturn(Types.VARCHAR);
        when(this.mockResultSetMetaData.getColumnType(3)).thenReturn(Types.BOOLEAN);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(3);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (1, 'foo', true), (2, 'bar', false), (3, 'cat', true)"));
    }

    @ValueSource(ints = { Types.VARCHAR, Types.TIME, Types.VARBINARY })
    @ParameterizedTest
    void testRewriteStringWithValueNull(final int type) throws AdapterException, SQLException {
        when(this.mockResultSet.getString(any())).thenReturn(null);
        mockOneRowWithOneColumnOfType(type);
        assertNullValueUsedInSelect();
    }

    private void mockOneRowWithOneColumnOfType(final int type) throws SQLException {
        when(this.mockResultSet.next()).thenReturn(true, false);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(type);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(this.mockResultSet.wasNull()).thenReturn(true);
    }

    private void assertNullValueUsedInSelect() throws AdapterException, SQLException {
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (NULL)"));
    }

    @Test
    void testRewriteBigIntWithValueNull() throws AdapterException, SQLException {
        when(this.mockResultSet.getInt(any())).thenReturn(0);
        mockOneRowWithOneColumnOfType(Types.BIGINT);
        assertNullValueUsedInSelect();
    }

    @Test
    void testRewriteTimestampWithValueNull() throws AdapterException, SQLException {
        when(this.mockResultSet.getString(any())).thenReturn(null);
        mockOneRowWithOneColumnOfType(Types.TIMESTAMP);
        assertNullValueUsedInSelect();
    }

    @Test
    void testRewriteDateWithValueNull() throws AdapterException, SQLException {
        when(this.mockResultSet.getString(any())).thenReturn(null);
        mockOneRowWithOneColumnOfType(Types.DATE);
        assertNullValueUsedInSelect();
    }

    @Test
    void testRewriteBooleanWithValueNull() throws AdapterException, SQLException {
        when(this.mockResultSet.getBoolean(any())).thenReturn(false);
        mockOneRowWithOneColumnOfType(Types.BOOLEAN);
        assertNullValueUsedInSelect();
    }

    @ValueSource(ints = { Types.DOUBLE, Types.NUMERIC })
    @ParameterizedTest
    void testRewriteNumericWithValueNull(final int type) throws AdapterException, SQLException {
        when(this.mockResultSet.getDouble(any())).thenReturn(0.0);
        mockOneRowWithOneColumnOfType(type);
        assertNullValueUsedInSelect();
    }
}