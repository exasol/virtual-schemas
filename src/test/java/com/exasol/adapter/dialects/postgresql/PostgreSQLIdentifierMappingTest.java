package com.exasol.adapter.dialects.postgresql;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PostgreSQLIdentifierMappingTest {
    @Test
    void testParseConvertToUpperCase() {
        assertThat(PostgreSQLIdentifierMapping.parse("CONVERT_TO_UPPER"),
                equalTo(PostgreSQLIdentifierMapping.CONVERT_TO_UPPER));
    }

    @Test
    void testParseConvertToPreserverOriginalCase() {
        assertThat(PostgreSQLIdentifierMapping.parse("PRESERVE_ORIGINAL_CASE"),
                equalTo(PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE));
    }

    @Test
    void testParseUnknownMappingThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> PostgreSQLIdentifierMapping.parse("UNKNOWN"));
    }
}