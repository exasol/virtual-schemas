package com.exasol.adapter.dialects.sybase;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class SybaseIdentifierTest {
    @ParameterizedTest
    @ValueSource(strings = { "my_underscore_table", "123 column one", "table@#$", "テスト", "таблица" })
    void testCreateValidIdentifier(final String identifier) {
        assertDoesNotThrow(() -> SybaseIdentifier.of(identifier));
    }

    @ParameterizedTest
    @ValueSource(strings = { "test[table]", "test`table", "\" table123" })
    void testCreateInvalidIdentifier(final String identifier) {
        assertThrows(AssertionError.class, () -> SybaseIdentifier.of(identifier));
    }

    @Test
    void testEqualsAndHashContract() {
        EqualsVerifier.simple().forClass(SybaseIdentifier.class).verify();
    }
}