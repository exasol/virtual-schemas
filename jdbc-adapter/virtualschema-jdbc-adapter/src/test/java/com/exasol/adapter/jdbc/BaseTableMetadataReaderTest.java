package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.TableMetadataMockUtils.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.TableMetadata;

@ExtendWith(MockitoExtension.class)
class BaseTableMetadataReaderTest {
    @Mock
    private Connection connectionMock;
    @Mock
    private DatabaseMetaData remoteMetadataMock;
    @Mock
    private ResultSet tablesMock;
    @Mock
    private ColumnMetadataReader columnMetadataReaderMock;

    @Test
    void testIsTableIncludedByMapping() throws SQLException {
        assertThat(createDefaultTableMetadataReader().isTableIncludedByMapping("any name"), equalTo(true));
    }

    @Test
    void testMapTables() throws SQLException {
        mockTableCount(this.tablesMock, 2);
        mockTableName(this.tablesMock, TABLE_A, TABLE_B);
        mockTableComment(this.tablesMock, TABLE_A_COMMENT, TABLE_B_COMMENT);
        mockTableWithColumnsOfType(this.tablesMock, this.columnMetadataReaderMock, TABLE_A, DataType.createBool());
        mockTableWithColumnsOfType(this.tablesMock, this.columnMetadataReaderMock, TABLE_B, DataType.createDate());
        final List<TableMetadata> tables = createDefaultTableMetadataReader().mapTables(this.tablesMock,
                Optional.empty());
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

    private TableMetadataReader createDefaultTableMetadataReader() {
        return new BaseTableMetadataReader(this.connectionMock, this.columnMetadataReaderMock,
                AdapterProperties.emptyProperties(), BaseIdentifierConverter.createDefault());
    }

    protected void mockConnection() throws SQLException {
        when(this.connectionMock.getMetaData()).thenReturn(this.remoteMetadataMock);
    }

    @Test
    void testMapTablesIgnoresTablesThatHaveNoColumns() throws SQLException {
        mockTableCount(this.tablesMock, 2);
        mockTableName(this.tablesMock, TABLE_A, TABLE_B);
        mockTableComment(this.tablesMock, TABLE_A_COMMENT, TABLE_B_COMMENT);
        mockTableWithColumnsOfType(this.tablesMock, this.columnMetadataReaderMock, TABLE_A, DataType.createBool());
        final List<TableMetadata> tables = createDefaultTableMetadataReader().mapTables(this.tablesMock,
                Optional.empty());
        final TableMetadata tableA = tables.get(0);
        assertAll(() -> assertThat(tables, iterableWithSize(1)), //
                () -> assertThat(tableA.getName(), equalTo(TABLE_A)));
    }
}