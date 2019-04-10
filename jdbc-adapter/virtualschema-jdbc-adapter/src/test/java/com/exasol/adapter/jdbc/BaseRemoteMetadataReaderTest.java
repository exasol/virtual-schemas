package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.TableMetadataMockUtils.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.metadata.*;

@ExtendWith(MockitoExtension.class)
class BaseRemoteMetadataReaderTest {
    private static final String IDENTIFIER_QUOTE_STRING = "identifier-quote-string";
    private static final String CATALOG_SEPARATOR = "catalog-separator";
    @Mock
    private Connection connectionMock;
    @Mock
    private DatabaseMetaData remoteMetadataMock;

    @BeforeEach
    void beforeEach() throws SQLException {
        when(this.connectionMock.getMetaData()).thenReturn(this.remoteMetadataMock);
    }

    @Test
    void testReadEmptyRemoteMetadata() throws RemoteMetadataReaderException, SQLException {
        mockGetAllTablesReturnsEmptyList();
        mockSupportingMetadata();
        assertThat(readMockedSchemaMetadata().getTables(), emptyIterableOf(TableMetadata.class));
    }

    private SchemaMetadata readMockedSchemaMetadata() {
        return readMockedSchemaMetadataWithProperties(AdapterProperties.emptyProperties());
    }

    protected SchemaMetadata readMockedSchemaMetadataWithProperties(final AdapterProperties properties) {
        final RemoteMetadataReader reader = new BaseRemoteMetadataReader(this.connectionMock, properties);
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
        mockSupportingMetadata();
        final SchemaMetadata metadata = readMockedSchemaMetadata();
        final List<TableMetadata> tables = metadata.getTables();
        final TableMetadata tableAMetadata = tables.get(0);
        final TableMetadata tableBMetadata = tables.get(1);
        final List<ColumnMetadata> columnsAMetadata = tableAMetadata.getColumns();
        final List<ColumnMetadata> columnsBMetadata = tableBMetadata.getColumns();
        assertAll(() -> assertThat(tables, iterableWithSize(2)),
                () -> assertThat(tableAMetadata.getName(), equalTo(TABLE_A)),
                () -> assertThat(columnsAMetadata, iterableWithSize(2)),
                () -> assertThat(tableBMetadata.getName(), equalTo(TABLE_B)),
                () -> assertThat(columnsBMetadata, iterableWithSize(3)));
    }

    private void mockGetColumnsCalls() throws SQLException {
        mockTableA();
        mockTableB();
    }

    private void mockTableA() throws SQLException {
        final ResultSet tableAColumns = Mockito.mock(ResultSet.class);
        when(tableAColumns.next()).thenReturn(true, true, false);
        when(tableAColumns.getString(BaseColumnMetadataReader.NAME_COLUMN)).thenReturn("COLUMN_A1", "COLUMN_A2");
        when(tableAColumns.getInt(BaseColumnMetadataReader.DATA_TYPE_COLUMN)).thenReturn(Types.BOOLEAN, Types.DATE);
        when(this.remoteMetadataMock.getColumns(null, null, TABLE_A, "%")).thenReturn(tableAColumns);
    }

    private void mockTableB() throws SQLException {
        final ResultSet tableBColumns = Mockito.mock(ResultSet.class);
        when(tableBColumns.next()).thenReturn(true, true, true, false);
        when(tableBColumns.getInt(BaseColumnMetadataReader.DATA_TYPE_COLUMN)).thenReturn(Types.BOOLEAN, Types.DOUBLE);
        when(tableBColumns.getString(BaseColumnMetadataReader.NAME_COLUMN)).thenReturn("COLUMN_B1", "COLUMN_B2",
                "COLUMN_B3");
        when(this.remoteMetadataMock.getColumns(null, null, TABLE_B, "%")).thenReturn(tableBColumns);
    }

    private void mockGetTableCalls() throws SQLException {
        final ResultSet tables = Mockito.mock(ResultSet.class);
        mockTableCount(tables, 2);
        mockTableName(tables, TABLE_A, TABLE_B);
        mockGetAllTables(tables);
    }

    private void mockGetAllTables(final ResultSet tables) throws SQLException {
        when(this.remoteMetadataMock.getTables(null, null, "%",
                RemoteMetadataReaderConstants.SUPPORTED_TABLE_TYPES.toArray(new String[0]))).thenReturn(tables);
    }

    protected void mockSupportingMetadata() throws SQLException {
        when(this.remoteMetadataMock.getCatalogSeparator()).thenReturn(CATALOG_SEPARATOR);
        when(this.remoteMetadataMock.getIdentifierQuoteString()).thenReturn(IDENTIFIER_QUOTE_STRING);
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
    }

    @Test
    void testReadRemoteDataSkippingFilteredTables() throws SQLException {
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(AdapterProperties.TABLE_FILTER_PROPERTY, TABLE_B);
        mockTableA();
        mockGetTableCalls();
        mockSupportingMetadata();
        final SchemaMetadata metadata = readMockedSchemaMetadataWithProperties(new AdapterProperties(rawProperties));
        final List<TableMetadata> tables = metadata.getTables();
        final TableMetadata tableAMetadata = tables.get(0);
        assertAll(() -> assertThat(tables, iterableWithSize(1)),
                () -> assertThat(tableAMetadata.getName(), equalTo(TABLE_A)));
    }

    @Test
    void testCreateSchemaAdapterNotes() throws SQLException {
        final RemoteMetadataReader reader = new BaseRemoteMetadataReader(this.connectionMock,
                AdapterProperties.emptyProperties());
        mockSupportingMetadata();
        final SchemaAdapterNotes notes = reader.getSchemaAdapterNotes();
        assertAll(() -> assertThat(notes.getCatalogSeparator(), equalTo(CATALOG_SEPARATOR)),
                () -> assertThat(notes.getIdentifierQuoteString(), equalTo(IDENTIFIER_QUOTE_STRING)),
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

    @Test
    void testReadRemoteMetadataWithAdapterNotes() throws RemoteMetadataReaderException, SQLException {
        final ResultSet tablesMock = Mockito.mock(ResultSet.class);
        mockTableCount(tablesMock, 1);
        mockTableName(tablesMock, TABLE_A);
        mockGetAllTables(tablesMock);
        mockSupportingMetadata();
        mockTableA();
        final SchemaMetadata metadata = readMockedSchemaMetadata();
        final List<TableMetadata> tables = metadata.getTables();
        final TableMetadata tableAMetadata = tables.get(0);
        final List<ColumnMetadata> columnsAMetadata = tableAMetadata.getColumns();
        assertAll(() -> assertThat(metadata.getAdapterNotes(), startsWith("{\"catalogSeparator\":")),
                () -> assertThat(tables, iterableWithSize(1)),
                () -> assertThat(tableAMetadata.getName(), equalTo(TABLE_A)),
                () -> assertThat(columnsAMetadata, iterableWithSize(2)));
    }
}