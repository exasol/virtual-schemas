package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
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
        when(this.statementMock.getMetaData()).thenReturn(this.resultSetMetadataMock);
        when(this.connectionMock.prepareStatement(any())).thenReturn(this.statementMock);
        final String columnDescription = "c1 BOOLEAN, c2 VARCHAR(20) UTF8";
        final ColumnMetadataReader columnMetadataReader = new BaseColumnMetadataReader(this.connectionMock,
                AdapterProperties.emptyProperties(), BaseIdentifierConverter.createDefault());
        final ResultSetMetadataReader reader = new ResultSetMetadataReader(this.connectionMock, columnMetadataReader);
        assertThat(reader.describeColumns("irrelevant"), equalTo(columnDescription));
    }
}