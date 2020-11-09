package com.exasol.adapter.dialects.impala;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import nl.jqno.equalsverifier.EqualsVerifier;

class ImpalaIdentifierTest {
    @ParameterizedTest
    @ValueSource(strings = { "_myunderscoretable", "123columnone", "テスト", "таблица" })
    void testCreateValidIdentifier(final String identifier) {
        assertDoesNotThrow(() -> ImpalaIdentifier.of(identifier));
    }

    @ParameterizedTest
    @ValueSource(strings = { "test`table", "`table`" })
    void testCreateInvalidIdentifier(final String identifier) {
        assertThrows(AssertionError.class, () -> ImpalaIdentifier.of(identifier));
    }

    @Test
    void testEqualsAndHashContract() {
        EqualsVerifier.simple().forClass(ImpalaIdentifier.class).verify();
    }
}