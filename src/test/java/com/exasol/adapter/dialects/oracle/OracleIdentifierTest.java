package com.exasol.adapter.dialects.oracle;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import nl.jqno.equalsverifier.EqualsVerifier;

class OracleIdentifierTest {
    @ParameterizedTest
    @ValueSource(strings = { "_myunderscoretable", "123columnone", "テスト", "таблица" })
    void testCreateValidIdentifier(final String identifier) {
        assertDoesNotThrow(() -> OracleIdentifier.of(identifier));
    }

    @ParameterizedTest
    @ValueSource(strings = { "\"testtable\"", "test\"table" })
    void testCreateInvalidIdentifier(final String identifier) {
        assertThrows(AssertionError.class, () -> OracleIdentifier.of(identifier));
    }

    @Test
    void testEqualsAndHashContract() {
        EqualsVerifier.simple().forClass(OracleIdentifier.class).verify();
    }
}