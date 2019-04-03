package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import com.exasol.adapter.metadata.*;

class BaseRemoteMetadataReaderTest {
    @Mock
    private Connection connectionMock;
    @Mock
    private DatabaseMetaData remoteMetadataMock;

    @BeforeEach
    void beforeEach() throws SQLException {
        MockitoAnnotations.initMocks(this);
        when(this.connectionMock.getMetaData()).thenReturn(this.remoteMetadataMock);
    }

    @Test
    void testReadEmptyRemoteMetadata() throws RemoteMetadataReaderException, SQLException {
        mockGetAllTablesReturnsEmptyList();
        assertThat(readMockedSchemaMetadata().getTables(), emptyIterableOf(TableMetadata.class));
    }

    private SchemaMetadata readMockedSchemaMetadata() {
        final RemoteMetadataReader reader = new BaseRemoteMetadataReader(this.connectionMock);
        return reader.readRemoteSchemaMetadata();
    }

    private void mockGetAllTablesReturnsEmptyList() throws SQLException {
        final ResultSet remoteTables = Mockito.mock(ResultSet.class);
        when(remoteTables.next()).thenReturn(false);
        mockGetAllTables(remoteTables);
    }

    @Test
    void testReadRemoteMetadata() throws RemoteMetadataReaderException, SQLException {
        mockGetColumnsCalls();
        mockGetTableCalls();
        final SchemaMetadata metadata = readMockedSchemaMetadata();
        final List<TableMetadata> tables = metadata.getTables();
        final TableMetadata tableAMetadata = tables.get(0);
        final TableMetadata tableBMetadata = tables.get(1);
        final List<ColumnMetadata> columnsAMetadata = tableAMetadata.getColumns();
        final List<ColumnMetadata> columnsBMetadata = tableBMetadata.getColumns();
        assertAll(() -> assertThat(tables, iterableWithSize(2)),
                () -> assertThat(tableAMetadata.getName(), equalTo("TABLE_A")),
                () -> assertThat(columnsAMetadata, iterableWithSize(2)),
                () -> assertThat(tableBMetadata.getName(), equalTo("TABLE_B")),
                () -> assertThat(columnsBMetadata, iterableWithSize(3)));
    }

    private void mockGetColumnsCalls() throws SQLException {
        mockTableA();
        mockTableB();
    }

    private void mockTableA() throws SQLException {
        final ResultSet tableAColumns = Mockito.mock(ResultSet.class);
        when(tableAColumns.next()).thenReturn(true, true, false);
        when(tableAColumns.getString("COLUMN_NAME")).thenReturn("COLUMN_A1", "COLUMN_A2");
        when(tableAColumns.getBoolean("NULLABLE")).thenReturn(true, false);
        when(this.remoteMetadataMock.getColumns(null, null, "TABLE_A", "%")).thenReturn(tableAColumns);
    }

    private void mockTableB() throws SQLException {
        final ResultSet tableBColumns = Mockito.mock(ResultSet.class);
        when(tableBColumns.next()).thenReturn(true, true, true, false);
        when(tableBColumns.getString("COLUMN_NAME")).thenReturn("COLUMN_B1", "COLUMN_B2", "COLUMN_B3");
        when(tableBColumns.getBoolean("NULLABLE")).thenReturn(false, true, false);
        when(this.remoteMetadataMock.getColumns(null, null, "TABLE_B", "%")).thenReturn(tableBColumns);
    }

    private void mockGetTableCalls() throws SQLException {
        final ResultSet tables = Mockito.mock(ResultSet.class);
        when(tables.next()).thenReturn(true, true, false);
        when(tables.getString("TABLE_NAME")).thenReturn("TABLE_A", "TABLE_B");
        mockGetAllTables(tables);
    }

    private void mockGetAllTables(final ResultSet tables) throws SQLException {
        when(this.remoteMetadataMock.getTables(null, null, "%", null)).thenReturn(tables);
    }
}