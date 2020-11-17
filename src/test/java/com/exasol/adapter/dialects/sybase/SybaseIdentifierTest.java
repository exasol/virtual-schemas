package com.exasol.adapter.dialects.sybase;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import nl.jqno.equalsverifier.EqualsVerifier;

class SybaseIdentifierTest {
    @ParameterizedTest
    @ValueSource(strings = { "my_underscore_table", "table@#$¥£", "TABLE_123" })
    void testCreateValidIdentifier(final String identifier) {
        assertDoesNotThrow(() -> SybaseIdentifier.of(identifier));
    }

    @ParameterizedTest
    @ValueSource(strings = { "test[table]", "test`table", "\" table123", "テスト", "таблица", "123 column one" })
    void testCreateInvalidIdentifier(final String identifier) {
        assertThrows(AssertionError.class, () -> SybaseIdentifier.of(identifier));
    }

    @Test
    void testEqualsAndHashContract() {
        EqualsVerifier.simple().forClass(SybaseIdentifier.class).verify();
    }
}