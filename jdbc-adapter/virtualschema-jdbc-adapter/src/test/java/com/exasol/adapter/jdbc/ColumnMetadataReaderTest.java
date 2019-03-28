package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.exasol.adapter.dialects.impl.ExasolSqlDialect;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;

class ColumnMetadataReaderTest {
    private static final String COLUMN_A = "COLUMN_A";
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
    void testMapColumnsSingleColumn() throws SQLException {
        mockDatatype(Types.BOOLEAN);
        final ColumnMetadata column = mapSingleMockedColumn();
        assertAll(() -> assertThat(column.getName(), equalTo(COLUMN_A)),
                () -> assertThat(column.getType(), equalTo(DataType.createBool())));
    }

    private void mockDatatype(final int typeId) throws SQLException {
        when(this.columnsMock.getInt(ColumnMetadataReader.DATA_TYPE)).thenReturn(typeId);
    }

    private ColumnMetadata mapSingleMockedColumn() throws SQLException {
        when(this.columnsMock.next()).thenReturn(true, false);
        when(this.columnsMock.getString(ColumnMetadataReader.COLUMN_NAME)).thenReturn(COLUMN_A);
        when(this.remoteMetadataMock.getColumns(null, null, "THE_TABLE", "%")).thenReturn(this.columnsMock);
        final List<ColumnMetadata> columns = mapMockedColumns(this.columnsMock);
        final ColumnMetadata column = columns.get(0);
        return column;
    }

    private List<ColumnMetadata> mapMockedColumns(final ResultSet columnsMock)
            throws RemoteMetadataReaderException, SQLException {
        when(this.remoteMetadataMock.getColumns(null, null, "THE_TABLE", "%")).thenReturn(columnsMock);
        final List<ColumnMetadata> columns = new ColumnMetadataReader(this.connectionMock, new ExasolSqlDialect(null))
                .mapColumns("THE_TABLE");
        return columns;
    }

    @Test
    void testParseBoolean() throws SQLException, RemoteMetadataReaderException {
        mockDatatype(Types.BOOLEAN);
        assertThat(mapSingleMockedColumn().getType().toString(), equalTo("BOOLEAN"));
    }

    @Test
    void testParseDate() throws SQLException, RemoteMetadataReaderException {
        mockDatatype(Types.DATE);
        assertThat(mapSingleMockedColumn().getType().toString(), equalTo("DATE"));
    }

    @Test
    void testParseDouble() throws SQLException, RemoteMetadataReaderException {
        mockDatatype(Types.DOUBLE);
        assertThat(mapSingleMockedColumn().getType().toString(), equalTo("DOUBLE"));
    }

    @Test
    void testParseChar() throws SQLException, RemoteMetadataReaderException {
        mockSize(20);
        mockDatatype(Types.CHAR);
        assertThat(mapSingleMockedColumn().getType().toString(), equalTo("CHAR(20) UTF8"));
    }

    private void mockSize(final int size) throws SQLException {
        when(this.columnsMock.getInt(ColumnMetadataReader.COLUMN_SIZE)).thenReturn(size);
    }

    @Test
    void testParseVarChar() throws SQLException {
        mockSize(40);
        mockDatatype(Types.VARCHAR);
        assertThat(mapSingleMockedColumn().getType().toString(), equalTo("VARCHAR(40) UTF8"));
    }

    @Test
    void testParseTimestamp() throws SQLException {
        mockDatatype(Types.TIMESTAMP);
        assertThat(mapSingleMockedColumn().getType().toString(), equalTo("TIMESTAMP"));
    }
}