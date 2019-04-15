package com.exasol.adapter.dialects.oracle;

import static com.exasol.adapter.jdbc.TableMetadataMockUtils.*;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.oracle.OracleTableMetadataReader;
import com.exasol.adapter.jdbc.ColumnMetadataReader;
import com.exasol.adapter.jdbc.TableMetadataReader;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.TableMetadata;

@ExtendWith(MockitoExtension.class)
class OracleTableMetadataReaderTest {
    private TableMetadataReader reader;
    @Mock
    private ResultSet tablesMock;
    @Mock
    private ColumnMetadataReader columnMetadataReaderMock;

    @BeforeEach
    void beforeEach() {
        this.reader = new OracleTableMetadataReader(this.columnMetadataReaderMock, AdapterProperties.emptyProperties());
    }

    @CsvSource({ "ANY_TABLE_NAME, true", "BIN$FOO, false" })
    @ParameterizedTest
    void testIsTableIncludedByMapping(final String tableName, final boolean expectedIncluded) {
        assertThat(this.reader.isTableIncludedByMapping(tableName), equalTo(expectedIncluded));
    }

    @Test
    void testTablesInTrashBinAreNotMapped() throws SQLException {
        mockTableCount(this.tablesMock, 3);
        mockTableName(this.tablesMock, TABLE_A, TABLE_B, "BIN$TRASHED");
        mockTableWithColumnsOfType(this.tablesMock, this.columnMetadataReaderMock, TABLE_A, DataType.createBool());
        mockTableWithColumnsOfType(this.tablesMock, this.columnMetadataReaderMock, TABLE_B, DataType.createBool());
        final List<String> tableNames = this.reader.mapTables(this.tablesMock) //
                .stream() //
                .map(TableMetadata::getName) //
                .collect(Collectors.toList());
        assertThat(tableNames, containsInAnyOrder(TABLE_A, TABLE_B));
    }
}