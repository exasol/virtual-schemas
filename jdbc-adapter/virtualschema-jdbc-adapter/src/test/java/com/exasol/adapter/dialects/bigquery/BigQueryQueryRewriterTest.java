package com.exasol.adapter.dialects.bigquery;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.sql.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.*;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;
import org.mockito.*;
import org.mockito.junit.jupiter.*;
import org.mockito.quality.*;

import com.exasol.*;
import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.*;

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
        when(this.mockResultSet.next()).thenReturn(true, false);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
    }

    @Test
    void testRewriteWithJdbcConnectionEmptyTable() throws AdapterException, SQLException {
        when(this.mockResultSet.next()).thenReturn(false);
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn("1");
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (1) WHERE false"));
    }

    @Test
    void testRewriteWithBigInt() throws AdapterException, SQLException {
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn("bigInt");
        when(this.mockResultSet.getInt("bigInt")).thenReturn(1);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(Types.BIGINT);
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
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (" + columnValue + ")"));
    }

    @Test
    void testRewriteWithBoolean() throws AdapterException, SQLException {
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn("boolean");
        when(this.mockResultSet.getBoolean("boolean")).thenReturn(true);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(Types.BOOLEAN);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (true)"));
    }

    @CsvSource({ "string, 12, hello", //
            "bytes, -3 ,D7A0FA33B3CDDF0EC29FC989CAB9AB512658F6E9A631FE5008610CBA24A64A5C", //
            "date, 91, 1111-01-01", //
            "time, 92, 12:10:09.000", //
            "timestamp, 93, 2014-09-27 20:30:00.450000", //
            "datetime, 93, 1111-01-01 12:10:09.000000" })
    @ParameterizedTest
    void testRewriteWithVarcharValues(final String columnName, final int type, final String columnValue)
            throws AdapterException, SQLException {
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn(columnName);
        when(this.mockResultSet.getString(columnName)).thenReturn(columnValue);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(type);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES ('" + columnValue + "')"));
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
}