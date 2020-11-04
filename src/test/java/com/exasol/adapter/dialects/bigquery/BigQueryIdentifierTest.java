package com.exasol.adapter.dialects.bigquery;

import org.junit.jupiter.api.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

class BigQueryIdentifierTest {
    @Test
    void testEqualsAndHashContract() {
        EqualsVerifier.simple().forClass(BigQueryIdentifier.class).verify();
    }
}