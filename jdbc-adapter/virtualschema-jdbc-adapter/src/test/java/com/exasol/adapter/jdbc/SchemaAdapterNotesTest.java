package com.exasol.adapter.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

class SchemaAdapterNotesTest {

    @Test
    void createSchemaAdapterNotes() {
        final SchemaAdapterNotes schemaAdapterNotes = SchemaAdapterNotes.builder().build();
        assertThat(schemaAdapterNotes, instanceOf(SchemaAdapterNotes.class));
    }
}