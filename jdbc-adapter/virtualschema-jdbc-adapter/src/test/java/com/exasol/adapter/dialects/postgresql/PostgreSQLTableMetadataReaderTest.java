package com.exasol.adapter.dialects.postgresql;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.PostgreSQLIdentifierMapping;
import com.exasol.adapter.jdbc.RemoteMetadataReaderException;

class PostgreSQLTableMetadataReaderTest {
    @CsvSource({ //
            "foobar, 'NONE', CONVERT_TO_UPPER, true", //
            "foobar, POSTGRESQL_UPPERCASE_TABLES, CONVERT_TO_UPPER, true", //
            "FooBar, POSTGRESQL_UPPERCASE_TABLES, PRESERVE_ORIGINAL_CASE, true", //
            "FooBar, 'NONE', PRESERVE_ORIGINAL_CASE, true" })
    @ParameterizedTest
    void testIsUppercaseTableIncludedByMapping(final String tableName, final String ignoreErrors,
            final PostgreSQLIdentifierMapping identifierMapping, final boolean included) {
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(PostgreSQLTableMetadataReader.IGNORE_ERRORS_PROPERTY, ignoreErrors);
        rawProperties.put(PostgreSQLTableMetadataReader.POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY,
                identifierMapping.toString());
        final PostgreSQLTableMetadataReader reader = new PostgreSQLTableMetadataReader(null,
                new AdapterProperties(rawProperties));
        assertThat(reader.isTableIncludedByMapping(tableName), equalTo(included));
    }

    @Test
    void testIsUppercaseTableIncludedByMappingWithConvertToUpperNotIgnoringUppercaseTablesThrowsException() {
        final PostgreSQLTableMetadataReader reader = new PostgreSQLTableMetadataReader(null,
                AdapterProperties.emptyProperties());
        assertThrows(RemoteMetadataReaderException.class, () -> reader.isTableIncludedByMapping("FooBar"));
    }

}