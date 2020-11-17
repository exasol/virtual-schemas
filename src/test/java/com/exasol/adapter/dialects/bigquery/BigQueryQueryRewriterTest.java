package com.exasol.adapter.dialects.bigquery;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.sql.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.jdbc.ConnectionFactory;
import com.exasol.adapter.sql.SqlStatement;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BigQueryQueryRewriterTest extends AbstractQueryRewriterTestBase {
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
    void beforeEach(@Mock final ConnectionFactory connectionFactoryMock) throws SQLException {
        final Connection connectionMock = this.mockConnection();
        this.statement = Mockito.mock(SqlStatement.class);
        when(connectionFactoryMock.getConnection()).thenReturn(connectionMock);
        final SqlDialectFactory factory = new BigQuerySqlDialectFactory();
        final SqlDialect dialect = factory.createSqlDialect(connectionFactoryMock, AdapterProperties.emptyProperties());
        final BaseRemoteMetadataReader metadataReader = new BaseRemoteMetadataReader(connectionMock,
                AdapterProperties.emptyProperties());
        this.queryRewriter = new BigQueryQueryRewriter(dialect, metadataReader, connectionFactoryMock);
        when(connectionMock.createStatement()).thenReturn(this.mockStatement);
        when(this.mockResultSet.getMetaData()).thenReturn(this.mockResultSetMetaData);
        when(this.mockStatement.executeQuery(any())).thenReturn(this.mockResultSet);
    }

    @Test
    void testRewriteWithJdbcConnectionEmptyTable() throws AdapterException, SQLException {
        when(this.mockResultSet.next()).thenReturn(false);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(5);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES(1, 1, 1, 1, 1) WHERE false"));
    }

    @CsvSource({ "float_col, 8, 105.0", //
            "float_col, 8, 99.4" //
    })
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

    @CsvSource({ "string_col, 12, hello, hello", //
            "string_col, 12, i'm, i\\'m", //
            "time_col, 92, 12:10:09.000, 12:10:09.000", //
            "numeric_col, 2, 22222.2222, 22222.2222", //
            "numeric_col, 2, 11.5, 11.5", //
            "date_col, 91, 1111-01-01, 01.01.1111", //
            "date_col, 91, 2019-12-3, 03.12.2019", //
            "date_col, 91, 2019-5-02, 02.05.2019" //
    })
    @ParameterizedTest
    void testRewriteWithStringValues(final String columnName, final int type, final String columnValue,
            final String resultValue) throws AdapterException, SQLException {
        assertQueryWithOneStringValue(columnName, type, columnValue, "SELECT * FROM VALUES ('" + resultValue + "')");
    }

    private void assertQueryWithOneStringValue(final String columnName, final int type, final String columnValue,
            final String query) throws SQLException, AdapterException {
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn(columnName);
        when(this.mockResultSet.getString(columnName)).thenReturn(columnValue);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(type);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(this.mockResultSet.next()).thenReturn(true, false);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo(query));
    }

    @Test
    void testRewriteWithBigInt() throws AdapterException, SQLException {
        assertQueryWithOneStringValue("bigint_col", Types.BIGINT, "123456", "SELECT * FROM VALUES (123456)");
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
        assertQueryWithOneStringValue("timestamp", 93, valueToConvert,
                "SELECT * FROM VALUES ('" + expectedValue + "')");
    }

    @Test
    void testRewriteWithJdbcConnectionWithThreeRows() throws AdapterException, SQLException {
        when(this.mockResultSet.next()).thenReturn(true, true, true, false);
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn("bigInt");
        when(this.mockResultSetMetaData.getColumnName(2)).thenReturn("varchar");
        when(this.mockResultSetMetaData.getColumnName(3)).thenReturn("boolean");
        when(this.mockResultSet.getString("bigInt")).thenReturn("1", "2", "3");
        when(this.mockResultSet.getString("varchar")).thenReturn("foo", "bar", "cat");
        when(this.mockResultSet.getBoolean("boolean")).thenReturn(true, false, true);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(Types.BIGINT);
        when(this.mockResultSetMetaData.getColumnType(2)).thenReturn(Types.VARCHAR);
        when(this.mockResultSetMetaData.getColumnType(3)).thenReturn(Types.BOOLEAN);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(3);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (1, 'foo', true), (2, 'bar', false), (3, 'cat', true)"));
    }

    @ValueSource(ints = { Types.VARCHAR, Types.TIME, Types.VARBINARY, Types.NUMERIC })
    @ParameterizedTest
    void testRewriteStringWithValueNull(final int type) throws AdapterException, SQLException {
        mockOneRowWithOneColumnOfType(type);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (CAST (NULL AS VARCHAR(4)))"));
    }

    private void mockOneRowWithOneColumnOfType(final int type) throws SQLException {
        when(this.mockResultSet.next()).thenReturn(true, false);
        when(this.mockResultSetMetaData.getColumnType(1)).thenReturn(type);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(this.mockResultSet.wasNull()).thenReturn(true);
    }

    @Test
    void testRewriteBigIntWithValueNull() throws AdapterException, SQLException {
        mockOneRowWithOneColumnOfType(Types.BIGINT);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (CAST (NULL AS DECIMAL(19,0)))"));
    }

    @Test
    void testRewriteTimestampWithValueNull() throws AdapterException, SQLException {
        mockOneRowWithOneColumnOfType(Types.TIMESTAMP);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (CAST (NULL AS VARCHAR(4)))"));
    }

    @Test
    void testRewriteDateWithValueNull() throws AdapterException, SQLException {
        mockOneRowWithOneColumnOfType(Types.DATE);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (CAST (NULL AS VARCHAR(4)))"));
    }

    @Test
    void testRewriteBooleanWithValueNull() throws AdapterException, SQLException {
        mockOneRowWithOneColumnOfType(Types.BOOLEAN);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (CAST (NULL AS BOOLEAN))"));
    }

    @Test
    void testRewriteDoubleWithValueNull() throws AdapterException, SQLException {
        mockOneRowWithOneColumnOfType(Types.DOUBLE);
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, AdapterProperties.emptyProperties()),
                equalTo("SELECT * FROM VALUES (CAST (NULL AS DOUBLE))"));
    }
}