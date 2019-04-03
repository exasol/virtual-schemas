package com.exasol.adapter.jdbc;

import com.exasol.adapter.metadata.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.sql.*;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

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

    @Test
    void testCreateSchemaAdapterNotes() throws SQLException {
        final RemoteMetadataReader reader = new BaseRemoteMetadataReader(this.connectionMock);
        when(this.remoteMetadataMock.getCatalogSeparator()).thenReturn("catalog-separator");
        when(this.remoteMetadataMock.getIdentifierQuoteString()).thenReturn("identifier-quote-string");
        when(this.remoteMetadataMock.storesLowerCaseIdentifiers()).thenReturn(true);
        when(this.remoteMetadataMock.storesUpperCaseIdentifiers()).thenReturn(true);
        when(this.remoteMetadataMock.storesMixedCaseIdentifiers()).thenReturn(true);
        when(this.remoteMetadataMock.supportsMixedCaseIdentifiers()).thenReturn(true);
        when(this.remoteMetadataMock.storesLowerCaseQuotedIdentifiers()).thenReturn(true);
        when(this.remoteMetadataMock.storesUpperCaseQuotedIdentifiers()).thenReturn(true);
        when(this.remoteMetadataMock.storesMixedCaseQuotedIdentifiers()).thenReturn(true);
        when(this.remoteMetadataMock.supportsMixedCaseQuotedIdentifiers()).thenReturn(true);
        when(this.remoteMetadataMock.nullsAreSortedAtEnd()).thenReturn(true);
        when(this.remoteMetadataMock.nullsAreSortedAtStart()).thenReturn(true);
        when(this.remoteMetadataMock.nullsAreSortedHigh()).thenReturn(true);
        when(this.remoteMetadataMock.nullsAreSortedLow()).thenReturn(true);

        final SchemaAdapterNotes notes = reader.getSchemaAdapterNotes();
        assertAll(() -> assertThat(notes.getCatalogSeparator(), equalTo("catalog-separator")),
              () -> assertThat(notes.getIdentifierQuoteString(), equalTo("identifier-quote-string")),

              () -> assertThat(notes.storesLowerCaseIdentifiers(), equalTo(true)),
              () -> assertThat(notes.storesUpperCaseIdentifiers(), equalTo(true)),
              () -> assertThat(notes.storesMixedCaseIdentifiers(), equalTo(true)),
              () -> assertThat(notes.supportsMixedCaseIdentifiers(), equalTo(true)),
              () -> assertThat(notes.storesLowerCaseQuotedIdentifiers(), equalTo(true)),
              () -> assertThat(notes.storesUpperCaseQuotedIdentifiers(), equalTo(true)),
              () -> assertThat(notes.storesMixedCaseQuotedIdentifiers(), equalTo(true)),
              () -> assertThat(notes.supportsMixedCaseQuotedIdentifiers(), equalTo(true)),
              () -> assertThat(notes.areNullsSortedAtStart(), equalTo(true)),
              () -> assertThat(notes.areNullsSortedAtEnd(), equalTo(true)),
              () -> assertThat(notes.areNullsSortedHigh(), equalTo(true)),
              () -> assertThat(notes.areNullsSortedLow(), equalTo(true)));
    }
}