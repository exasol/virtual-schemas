package com.exasol.adapter.adapternotes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.json.JSONException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import com.exasol.adapter.AdapterException;

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
          + "\"areNullsSortedAtEnd\":false," //
          + "\"areNullsSortedAtStart\":false," //
          + "\"areNullsSortedHigh\":false," //
          + "\"areNullsSortedLow\":false}";

    @Test
    void convertToJsonWithDefaultValues() throws JSONException {
        JSONAssert.assertEquals(SERIALIZED_STRING,
              this.schemaAdapterNotesJsonConverter.convertToJson(SchemaAdapterNotes.builder().build()), false);
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