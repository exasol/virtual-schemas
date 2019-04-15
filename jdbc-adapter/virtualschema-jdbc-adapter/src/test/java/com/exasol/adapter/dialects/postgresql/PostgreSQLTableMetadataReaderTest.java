package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.jdbc.TableMetadataMockUtils.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.PostgreSQLIdentifierMapping;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.TableMetadata;

@ExtendWith(MockitoExtension.class)
class PostgreSQLTableMetadataReaderTest {
    @Mock
    private ResultSet tablesMock;
    @Mock
    private ColumnMetadataReader columnMetadataReaderMock;

    @Test
    void testMapTablePreservingIdentifierCase() throws SQLException {
        mockTableCount(this.tablesMock, 1);
        mockTableName(this.tablesMock, "FooBar");
        mockTableWithColumnsOfType(this.tablesMock, this.columnMetadataReaderMock, "FooBar", DataType.createBool());
        final TableMetadata table = mapSingleTable(PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE, true);
        assertThat(table.getName(), equalTo("FooBar"));
    }

    private TableMetadata mapSingleTable(final PostgreSQLIdentifierMapping identifierMapping,
            final boolean ignoreUpperCaseTables) throws SQLException {
        final Map<String, String> rawProperties = new HashMap<>();
        setIgnoreUpperCaseTablesProperty(ignoreUpperCaseTables, rawProperties);
        setIdentifierMappingProperty(identifierMapping, rawProperties);
        final TableMetadataReader reader = new PostgreSQLTableMetadataReader(this.columnMetadataReaderMock,
                new AdapterProperties(rawProperties));
        final List<TableMetadata> tables = reader.mapTables(this.tablesMock);
        final TableMetadata table = tables.get(0);
        return table;
    }

    private void setIdentifierMappingProperty(final PostgreSQLIdentifierMapping identifierMapping,
            final Map<String, String> rawProperties) {
        rawProperties.put(PostgreSQLTableMetadataReader.POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY,
                identifierMapping.toString());
    }

    private void setIgnoreUpperCaseTablesProperty(final boolean ignoreUpperCaseTables,
            final Map<String, String> rawProperties) {
        if (ignoreUpperCaseTables) {
            rawProperties.put(PostgreSQLTableMetadataReader.IGNORE_ERRORS_PROPERTY,
                    PostgreSQLTableMetadataReader.POSTGRESQL_UPPERCASE_TABLES_SWITCH);
        }
    }

    @Test
    void testMapTableConvertingUnquotedIdentifierToUpperCase() throws SQLException {
        mockTableCount(this.tablesMock, 1);
        mockTableName(this.tablesMock, "FooBar");
        mockTableWithColumnsOfType(this.tablesMock, this.columnMetadataReaderMock, "FooBar", DataType.createBool());
        final TableMetadata table = mapSingleTable(PostgreSQLIdentifierMapping.CONVERT_TO_UPPER, true);
        assertThat(table.getName(), equalTo("FOOBAR"));
    }

    @Test
    void testMapTableSkipUpperCaseIfQuotedIdentifier() throws SQLException {
        final String quotedTableName = "\"FooBar\"";
        mockTableCount(this.tablesMock, 1);
        mockTableName(this.tablesMock, quotedTableName);
        mockTableWithColumnsOfType(this.tablesMock, this.columnMetadataReaderMock, quotedTableName,
                DataType.createBool());
        final TableMetadata table = mapSingleTable(PostgreSQLIdentifierMapping.CONVERT_TO_UPPER, true);
        assertThat(table.getName(), equalTo("\"FooBar\""));
    }

    @Test
    void testMapTableWithUpperCaseCharactersThrowsExceptionIfIgnoringIsOff() throws SQLException {
        mockTableName(this.tablesMock, "FooBar");
        assertThrows(RemoteMetadataReaderException.class,
                () -> mapSingleTable(PostgreSQLIdentifierMapping.CONVERT_TO_UPPER, false));
    }
}