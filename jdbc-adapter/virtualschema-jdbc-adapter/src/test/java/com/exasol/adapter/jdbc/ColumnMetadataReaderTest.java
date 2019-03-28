package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.DataType.ExaCharset;

class ColumnMetadataReaderTest {
    @Mock
    private Connection connectionMock;
    @Mock
    private DatabaseMetaData remoteMetadataMock;
    @Mock
    private ResultSet columnsMock;

    @BeforeEach
    void beforeEach() throws SQLException {
        MockitoAnnotations.initMocks(this);
        when(this.connectionMock.getMetaData()).thenReturn(this.remoteMetadataMock);
    }

    @Test
    void testMapColumnsSingleColumn() throws SQLException, RemoteMetadataReaderException {
        when(this.columnsMock.next()).thenReturn(true, false);
        when(this.columnsMock.getString("COLUMN_NAME")).thenReturn("COLUMN_A");
        when(this.columnsMock.getString("DATA_TYPE")).thenReturn("VARCHAR(20)");
        when(this.columnsMock.getBoolean("NULLABLE")).thenReturn(false);
        final List<ColumnMetadata> columns = mappedMockedColumns(this.columnsMock);
        final ColumnMetadata column = columns.get(0);
        assertAll(() -> assertThat(columns, iterableWithSize(2)),
                () -> assertThat(column.getName(), equalTo("COLUMN_A")),
                () -> assertThat(column.getType(), equalTo(DataType.createVarChar(20, ExaCharset.UTF8))));
    }

    private List<ColumnMetadata> mappedMockedColumns(final ResultSet columnsMock)
            throws RemoteMetadataReaderException, SQLException {
        when(this.remoteMetadataMock.getColumns(null, null, "THE_TABLE", "%")).thenReturn(columnsMock);
        final List<ColumnMetadata> columns = new ColumnMetadataReader(this.connectionMock).mapColumns("THE_TABLE");
        return columns;
    }

    @CsvSource({ //
            "BOOLEAN, BOOLEAN", //
            "CHAR, CHAR(20) UTF8", //
            "DOUBLE, DOUBLE" //
    })
    @ParameterizedTest
    void testParse(final String datatypeAsString, final String expected)
            throws SQLException, RemoteMetadataReaderException {
        when(this.columnsMock.next()).thenReturn(true, false);
        when(this.columnsMock.getString(ColumnMetadataReader.COLUMN_NAME)).thenReturn("COLUMN_A");
        when(this.columnsMock.getString(ColumnMetadataReader.DATA_TYPE)).thenReturn(datatypeAsString);
        when(this.columnsMock.getInt(ColumnMetadataReader.COLUMN_SIZE)).thenReturn(20);
        when(this.remoteMetadataMock.getColumns(null, null, "THE_TABLE", "%")).thenReturn(this.columnsMock);
        final List<ColumnMetadata> columns = mappedMockedColumns(this.columnsMock);
        final ColumnMetadata column = columns.get(0);
        assertThat(column.getType().toString(), equalTo(expected));
    }
}