package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.TableMetadataMockUtils.*;
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

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect.IdentifierCaseHandling;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.TableMetadata;

class BaseTableMetadataReaderTest {
    @Mock
    private Connection connectionMock;
    @Mock
    private DatabaseMetaData remoteMetadataMock;
    @Mock
    private ResultSet tablesMock;
    @Mock
    private ColumnMetadataReader columnMetadataReaderMock;

    @BeforeEach
    void beforeEach() throws SQLException {
        MockitoAnnotations.initMocks(this);
        when(this.connectionMock.getMetaData()).thenReturn(this.remoteMetadataMock);
    }

    @Test
    void testIsTableIncludedByMapping() {
        final TableMetadataReader reader = new BaseTableMetadataReader(null, AdapterProperties.emptyProperties());
        assertThat(reader.isTableIncludedByMapping("any name"), equalTo(true));
    }

    @Test
    void testMapTables() throws SQLException {
        mockTableCount(this.tablesMock, 2);
        mockTableName(this.tablesMock, TABLE_A, TABLE_B);
        mockTableComment(this.tablesMock, TABLE_A_COMMENT, TABLE_B_COMMENT);
        mockTableWithColumnsOfType(this.tablesMock, this.columnMetadataReaderMock, TABLE_A, DataType.createBool());
        mockTableWithColumnsOfType(this.tablesMock, this.columnMetadataReaderMock, TABLE_B, DataType.createDate());
        final TableMetadataReader reader = new BaseTableMetadataReader(this.columnMetadataReaderMock,
                AdapterProperties.emptyProperties());
        final List<TableMetadata> tables = reader.mapTables(this.tablesMock);
        final TableMetadata tableA = tables.get(0);
        final TableMetadata tableB = tables.get(1);
        assertAll(() -> assertThat(tables, iterableWithSize(2)), //
                () -> assertThat(tableA.getName(), equalTo(TABLE_A)),
                () -> assertThat(tableA.getComment(), equalTo(TABLE_A_COMMENT)),
                () -> assertThat(tableA.getAdapterNotes(),
                        equalTo(BaseTableMetadataReader.DEFAULT_TABLE_ADAPTER_NOTES)),
                () -> assertThat(tableA.getColumns().get(0).getName(), equalTo(COLUMN_A1)),
                () -> assertThat(tableB.getName(), equalTo(TABLE_B)),
                () -> assertThat(tableB.getComment(), equalTo(TABLE_B_COMMENT)),
                () -> assertThat(tableB.getAdapterNotes(),
                        equalTo(BaseTableMetadataReader.DEFAULT_TABLE_ADAPTER_NOTES)),
                () -> assertThat(tableB.getColumns().get(0).getName(), equalTo(COLUMN_B1)));
    }

    @CsvSource({ "INTERPRET_AS_LOWER, INTERPRET_AS_LOWER, true", //
            "INTERPRET_AS_LOWER, INTERPRET_AS_UPPER, false", //
            "INTERPRET_AS_LOWER, INTERPRET_CASE_SENSITIVE, false", //
            "INTERPRET_AS_UPPER, INTERPRET_AS_UPPER, true", //
            "INTERPRET_AS_UPPER, INTERPRET_AS_LOWER, false", //
            "INTERPRET_AS_UPPER, INTERPRET_CASE_SENSITIVE, false", //
            "INTERPRET_CASE_SENSITIVE, INTERPRET_AS_LOWER, false", //
            "INTERPRET_CASE_SENSITIVE, INTERPRET_AS_UPPER, false", //
            "INTERPRET_CASE_SENSITIVE, INTERPRET_CASE_SENSITIVE, false" })
    @ParameterizedTest
    void testAdjustIdentifierCase(final IdentifierCaseHandling unquotedIdentifierHandling,
            final IdentifierCaseHandling quotedIdentifierHandling, final boolean resultShouldBeUpperCase) {
        final TableMetadataReader reader = new DummyTableMetadataReader(this.columnMetadataReaderMock,
                unquotedIdentifierHandling, quotedIdentifierHandling);
        assertThat(reader.adjustIdentifierCase("text"), equalTo(resultShouldBeUpperCase ? "TEXT" : "text"));
    }

    private static class DummyTableMetadataReader extends BaseTableMetadataReader {
        private final IdentifierCaseHandling unquotedIdentifierCaseHandling;
        private final IdentifierCaseHandling quotedIdentifierCaseHandling;

        public DummyTableMetadataReader(final ColumnMetadataReader columnMetadataReader,
                final IdentifierCaseHandling unquotedIdentifierCaseHandling,
                final IdentifierCaseHandling quotedIdentifierCaseHandl) {
            super(columnMetadataReader, AdapterProperties.emptyProperties());
            this.quotedIdentifierCaseHandling = quotedIdentifierCaseHandl;
            this.unquotedIdentifierCaseHandling = unquotedIdentifierCaseHandling;

        }

        @Override
        public IdentifierCaseHandling getUnquotedIdentifierCaseHandling() {
            return this.unquotedIdentifierCaseHandling;
        }

        @Override
        public IdentifierCaseHandling getQuotedIdentifierCaseHandling() {
            return this.quotedIdentifierCaseHandling;
        }
    }

    @Test
    void testMapTablesIgnoresTablesThatHaveNoColumns() throws SQLException {
        mockTableCount(this.tablesMock, 2);
        mockTableName(this.tablesMock, TABLE_A, TABLE_B);
        mockTableComment(this.tablesMock, TABLE_A_COMMENT, TABLE_B_COMMENT);
        mockTableWithColumnsOfType(this.tablesMock, this.columnMetadataReaderMock, TABLE_A, DataType.createBool());
        final TableMetadataReader reader = new BaseTableMetadataReader(this.columnMetadataReaderMock,
                AdapterProperties.emptyProperties());
        final List<TableMetadata> tables = reader.mapTables(this.tablesMock);
        final TableMetadata tableA = tables.get(0);
        assertAll(() -> assertThat(tables, iterableWithSize(1)), //
                () -> assertThat(tableA.getName(), equalTo(TABLE_A)));
    }
}