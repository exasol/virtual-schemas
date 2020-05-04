package com.exasol.adapter.dialects.postgresql;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierCaseHandling;
import com.exasol.adapter.dialects.IdentifierConverter;

class PostgreSQLMetadataReaderTest {
    private PostgreSQLMetadataReader reader;

    @BeforeEach
    void beforeEach() {
        this.reader = new PostgreSQLMetadataReader(null, AdapterProperties.emptyProperties());
    }

    @Test
    void testGetTableMetadataReader() {
        assertThat(this.reader.getTableMetadataReader(), instanceOf(PostgreSQLTableMetadataReader.class));
    }

    @Test
    void testGetColumnMetadataReader() {
        assertThat(this.reader.getColumnMetadataReader(), instanceOf(PostgreSQLColumnMetadataReader.class));
    }

    @Test
    void testGetIdentifierConverter() {
        final IdentifierConverter identifierConverter = this.reader.getIdentifierConverter();
        assertAll(() -> assertThat(identifierConverter, instanceOf(PostgreSQLIdentifierConverter.class)),
                () -> assertThat(identifierConverter.getQuotedIdentifierHandling(),
                        equalTo(IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE)),
                () -> assertThat(identifierConverter.getUnquotedIdentifierHandling(),
                        equalTo(IdentifierCaseHandling.INTERPRET_AS_LOWER)));
    }
}