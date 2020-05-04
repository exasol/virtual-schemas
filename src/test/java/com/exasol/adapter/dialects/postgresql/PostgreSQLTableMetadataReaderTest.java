package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.AdapterProperties.IGNORE_ERRORS_PROPERTY;
import static com.exasol.adapter.dialects.postgresql.PostgreSQLSqlDialect.POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.jdbc.RemoteMetadataReaderException;

class PostgreSQLTableMetadataReaderTest {
    private Map<String, String> rawProperties;

    @BeforeEach
    void beforeEach() {
        this.rawProperties = new HashMap<>();
    }

    @CsvSource({ //
            "foobar    , NONE                       , CONVERT_TO_UPPER      , true", //
            "foobar    , POSTGRESQL_UPPERCASE_TABLES, CONVERT_TO_UPPER      , true", //
            "FooBar    , POSTGRESQL_UPPERCASE_TABLES, PRESERVE_ORIGINAL_CASE, true", //
            "FooBar    , NONE                       , PRESERVE_ORIGINAL_CASE, true", //
            "\"FooBar\", POSTGRESQL_UPPERCASE_TABLES, PRESERVE_ORIGINAL_CASE, true", //
            "\"FooBar\", NONE                       , PRESERVE_ORIGINAL_CASE, true" //
    })
    @ParameterizedTest
    void testIsUppercaseTableIncludedByMapping(final String tableName, final String ignoreErrors,
            final PostgreSQLIdentifierMapping identifierMapping, final boolean included) {
        ignoreErrors(ignoreErrors);
        selectIdentifierMapping(identifierMapping);
        assertThat(createTableMetadataReader().isTableIncludedByMapping(tableName), equalTo(included));
    }

    private void ignoreErrors(final String ignoreErrors) {
        this.rawProperties.put(IGNORE_ERRORS_PROPERTY, ignoreErrors);
    }

    private void selectIdentifierMapping(final PostgreSQLIdentifierMapping identifierMapping) {
        this.rawProperties.put(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY, identifierMapping.toString());
    }

    private PostgreSQLTableMetadataReader createTableMetadataReader() {
        final AdapterProperties properties = new AdapterProperties(this.rawProperties);
        return new PostgreSQLTableMetadataReader(null, null, properties, BaseIdentifierConverter.createDefault());
    }

    @Test
    void testIsUppercaseTableIncludedByMappingWithIgnoringUppercaseTables() {
        ignoreErrors("POSTGRESQL_UPPERCASE_TABLES");
        final PostgreSQLTableMetadataReader reader = createTableMetadataReader();
        assertThat(reader.isTableIncludedByMapping("\"FooBar\""), equalTo(false));
    }

    @Test
    void testIsUppercaseTableIncludedByMappingWithConvertToUpperNotIgnoringUppercaseTablesThrowsException() {

        final PostgreSQLTableMetadataReader reader = createTableMetadataReader();
        assertThrows(RemoteMetadataReaderException.class, () -> reader.isTableIncludedByMapping("\"FooBar\""));
    }
}