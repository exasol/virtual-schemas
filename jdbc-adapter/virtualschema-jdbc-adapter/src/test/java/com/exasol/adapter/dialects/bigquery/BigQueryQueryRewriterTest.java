package com.exasol.adapter.dialects.bigquery;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.sql.*;
import java.util.*;

import com.exasol.adapter.sql.*;
import org.junit.jupiter.api.*;

import com.exasol.*;
import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;

class BigQueryQueryRewriterTest extends AbstractQueryRewriterTest {
    private AdapterProperties properties;
    private QueryRewriter queryRewriter;
    private final ResultSet mockResultSet = mock(ResultSet.class);
    private ResultSetMetaData mockResultSetMetaData;
    private final Statement mockStatement = mock(Statement.class);

    @BeforeEach
    void beforeEach() throws SQLException {
        this.exaMetadata = mock(ExaMetadata.class);
        this.rawProperties = new HashMap<>();
        final Connection connectionMock = this.mockConnection();
        this.setUserPasswordAndConnectionString();
        this.properties = new AdapterProperties(this.rawProperties);
        final SqlDialect dialect = new BigQuerySqlDialect(connectionMock, this.properties);
        final BaseRemoteMetadataReader metadataReader = new BaseRemoteMetadataReader(connectionMock, this.properties);

        this.mockResultSetMetaData = mock(ResultSetMetaData.class);
        this.queryRewriter = new BigQueryQueryRewriter(dialect, metadataReader, connectionMock);
        this.mockResultSetMetaData = mock(ResultSetMetaData.class);
        when(connectionMock.createStatement()).thenReturn(this.mockStatement);
        when(this.mockResultSet.getMetaData()).thenReturn(this.mockResultSetMetaData);
        when(this.mockStatement.executeQuery(any())).thenReturn(this.mockResultSet);
    }

    @Test
    void testRewriteWithJdbcConnection() throws AdapterException, SQLException {
        this.statement = this.createSimpleSelectStatement();
        when(this.mockResultSet.next()).thenReturn(true, false);
        when(this.mockResultSet.last()).thenReturn(true);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(1);
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn("1");
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, this.properties),
                equalTo("SELECT * FROM (VALUES (1))"));
    }

    @Test
    void testRewriteWithJdbcConnection2() throws AdapterException, SQLException {
        this.statement = mock(SqlStatement.class);
        when(this.statement.accept(any())).thenReturn("SELECT * FROM (VALUES (1, 'foo'), (2, 'bar'))");
        when(this.mockResultSet.next()).thenReturn(true, true, false);
        when(this.mockResultSet.last()).thenReturn(false, false, true);
        when(this.mockResultSet.getInt(1)).thenReturn(1, 2);
        when(this.mockResultSet.getString(2)).thenReturn("foo", "bar");
        when(this.mockResultSet.getType()).thenReturn(Types.INTEGER, Types.VARCHAR);
        when(this.mockResultSetMetaData.getColumnCount()).thenReturn(2);
        when(this.mockResultSetMetaData.getColumnName(1)).thenReturn("1");
        when(this.mockResultSetMetaData.getColumnName(2)).thenReturn("2");
        assertThat(this.queryRewriter.rewrite(this.statement, this.exaMetadata, this.properties),
                equalTo("SELECT * FROM (VALUES (1, 'foo'), (2, 'bar'))"));
    }
}