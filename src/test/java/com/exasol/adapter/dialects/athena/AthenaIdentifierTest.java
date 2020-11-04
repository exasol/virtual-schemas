package com.exasol.adapter.dialects.athena;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import nl.jqno.equalsverifier.EqualsVerifier;

class AthenaIdentifierTest {
    @ParameterizedTest
    @ValueSource(strings = { "_myunderscoretable", "123columnone", "テスト", "таблица" })
    void testCreateValidIdentifier(final String identifier) {
        assertDoesNotThrow(() -> AthenaIdentifier.of(identifier));
    }

    @ParameterizedTest
    @ValueSource(strings = { "test.table", "test`table", "\" table123" })
    void testCreateInvalidIdentifier(final String identifier) {
        assertThrows(AssertionError.class, () -> AthenaIdentifier.of(identifier));
    }

    @Test
    void testEqualsAndHashContract() {
        EqualsVerifier.simple().forClass(AthenaIdentifier.class).verify();
    }
}