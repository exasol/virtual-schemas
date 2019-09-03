package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.sql.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;

@ExtendWith(MockitoExtension.class)
class ResultSetMetadataReaderTest {
    @Mock
    private ResultSetMetaData resultSetMetadataMock;
    @Mock
    private Connection connectionMock;
    @Mock
    private PreparedStatement statementMock;

    @Test
    void testDescribeColumn() throws SQLException {
        when(this.resultSetMetadataMock.getColumnCount()).thenReturn(2);
        when(this.resultSetMetadataMock.getColumnType(1)).thenReturn(Types.BOOLEAN);
        when(this.resultSetMetadataMock.getColumnType(2)).thenReturn(Types.VARCHAR);
        when(this.resultSetMetadataMock.getPrecision(1)).thenReturn(0);
        when(this.resultSetMetadataMock.getPrecision(2)).thenReturn(20);
        final String columnDescription = "c1 BOOLEAN, c2 VARCHAR(20) UTF8";
        assertThat(getReader().describeColumns("irrelevant"), equalTo(columnDescription));
    }

    public ResultSetMetadataReader getReader() throws SQLException {
        when(this.statementMock.getMetaData()).thenReturn(this.resultSetMetadataMock);
        when(this.connectionMock.prepareStatement(any())).thenReturn(this.statementMock);
        final ColumnMetadataReader columnMetadataReader = new BaseColumnMetadataReader(this.connectionMock,
                AdapterProperties.emptyProperties(), BaseIdentifierConverter.createDefault());
        return new ResultSetMetadataReader(this.connectionMock, columnMetadataReader);
    }

    @Test
    void testDescribeColumnThrowsExceptionIfUnsupportedColumnContained() throws SQLException {
        when(this.resultSetMetadataMock.getColumnCount()).thenReturn(4);
        when(this.resultSetMetadataMock.getColumnType(1)).thenReturn(Types.BOOLEAN);
        when(this.resultSetMetadataMock.getColumnType(2)).thenReturn(Types.BLOB);
        when(this.resultSetMetadataMock.getColumnType(3)).thenReturn(Types.DATE);
        when(this.resultSetMetadataMock.getColumnType(4)).thenReturn(Types.BLOB);
        final RemoteMetadataReaderException thrown = assertThrows(RemoteMetadataReaderException.class,
                () -> getReader().describeColumns("FOOBAR"));
        assertThat(thrown.getMessage(), startsWith("Unsupported data type(s) in column(s) 2, 4"));
    }
}