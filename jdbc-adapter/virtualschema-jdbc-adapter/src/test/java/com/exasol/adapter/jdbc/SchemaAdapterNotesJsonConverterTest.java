package com.exasol.adapter.jdbc;

import com.exasol.adapter.AdapterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaAdapterNotesJsonConverterTest {
    private SchemaAdapterNotesJsonConverter schemaAdapterNotesJsonConverter;

    @BeforeEach
    void setUp() {
        this.schemaAdapterNotesJsonConverter = SchemaAdapterNotesJsonConverter.getInstance();
    }

    private static final String SERIALIZED_STRING = "{\"catalogSeparator\":\".\"," //
          + "\"identifierQuoteString\":\"\\\"\"," //
          + "\"storesLowerCaseIdentifiers\":false," //
          + "\"storesUpperCaseIdentifiers\":false," //
          + "\"storesMixedCaseIdentifiers\":false,"  //
          + "\"supportsMixedCaseIdentifiers\":false," //
          + "\"storesLowerCaseQuotedIdentifiers\":false," //
          + "\"storesUpperCaseQuotedIdentifiers\":false," //
          + "\"storesMixedCaseQuotedIdentifiers\":false," //
          + "\"supportsMixedCaseQuotedIdentifiers\":false," //
          + "\"nullsAreSortedAtEnd\":false," //
          + "\"nullsAreSortedAtStart\":false," //
          + "\"nullsAreSortedHigh\":false," //
          + "\"nullsAreSortedLow\":false}";

    @Test
    void convertToJsonWithDefaultValues() {
        assertThat(this.schemaAdapterNotesJsonConverter.convertToJson(SchemaAdapterNotes.builder().build()),
              equalTo(SERIALIZED_STRING));
    }

    @Test
    void deserializeWithDefaultValues() throws AdapterException {
        assertThat(
              this.schemaAdapterNotesJsonConverter.convertFromJsonToSchemaAdapterNotes(SERIALIZED_STRING, "test_name"),
              equalTo(SchemaAdapterNotes.builder().build()));
    }

    @Test
    void deserializeThrowsExceptionWithEmptyAdapterNotes() {
        assertThrows(AdapterException.class,
              () -> this.schemaAdapterNotesJsonConverter.convertFromJsonToSchemaAdapterNotes("", ""));
    }

    @Test
    void deserializeThrowsExceptionWithWrongAdapterNotes() {
        assertThrows(AdapterException.class,
              () -> this.schemaAdapterNotesJsonConverter.convertFromJsonToSchemaAdapterNotes("testNotes", ""));
    }

}