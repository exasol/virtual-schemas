package com.exasol.adapter.adapternotes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.junit.jupiter.api.Test;

class SchemaAdapterNotesTest {
    private static final String CATALOG_SEPARATOR = ".";
    private static final String IDENTIFIER_QUOTE_STRING = "\"";
    private static final boolean STORES_LOWER_CASE_IDENTIFIERS = true;
    private static final boolean STORES_UPPER_CASE_IDENTIFIERS = false;
    private static final boolean STORES_MIXED_CASE_IDENTIFIERS = false;
    private static final boolean SUPPORTS_MIXED_CASE_IDENTIFIERS = true;
    private static final boolean STORES_LOWER_CASE_QUOTED_IDENTIFIERS = true;
    private static final boolean STORES_UPPER_CASE_QUOTED_IDENTIFIERS = false;
    private static final boolean STORES_MIXED_CASE_QUOTED_IDENTIFIERS = false;
    private static final boolean SUPPORTS_MIXED_CASE_QUOTED_IDENTIFIERS = true;
    private static final boolean NULLS_ARE_SORTED_AT_END = true;
    private static final boolean NULLS_ARE_SORTED_AT_START = false;
    private static final boolean NULLS_ARE_SORTED_HIGH = true;
    private static final boolean NULLS_ARE_SORTED_LOW = false;

    @Test
    void createSchemaAdapterNotes() {
        final SchemaAdapterNotes schemaAdapterNotes = SchemaAdapterNotes.builder().build();
        assertThat(schemaAdapterNotes, instanceOf(SchemaAdapterNotes.class));
    }

    @Test
    void createSchemaAdapterNotesWithAllFields() {
        final SchemaAdapterNotes schemaAdapterNotes = SchemaAdapterNotes.builder() //
              .catalogSeparator(CATALOG_SEPARATOR) //
              .identifierQuoteString(IDENTIFIER_QUOTE_STRING) //
              .storesLowerCaseIdentifiers(STORES_LOWER_CASE_IDENTIFIERS) //
              .storesUpperCaseIdentifiers(STORES_UPPER_CASE_IDENTIFIERS) //
              .storesMixedCaseIdentifiers(STORES_MIXED_CASE_IDENTIFIERS) //
              .supportsMixedCaseIdentifiers(SUPPORTS_MIXED_CASE_IDENTIFIERS) //
              .storesLowerCaseQuotedIdentifiers(STORES_LOWER_CASE_QUOTED_IDENTIFIERS) //
              .storesUpperCaseQuotedIdentifiers(STORES_UPPER_CASE_QUOTED_IDENTIFIERS) //
              .storesMixedCaseQuotedIdentifiers(STORES_MIXED_CASE_QUOTED_IDENTIFIERS) //
              .supportsMixedCaseQuotedIdentifiers(SUPPORTS_MIXED_CASE_QUOTED_IDENTIFIERS) //
              .areNullsSortedAtEnd(NULLS_ARE_SORTED_AT_END) //
              .areNullsSortedAtStart(NULLS_ARE_SORTED_AT_START) //
              .areNullsSortedHigh(NULLS_ARE_SORTED_HIGH) //
              .areNullsSortedLow(NULLS_ARE_SORTED_LOW) //
              .build();
        assertAll(() -> assertThat(schemaAdapterNotes.getCatalogSeparator(), equalTo(CATALOG_SEPARATOR)),
              () -> assertThat(schemaAdapterNotes.getIdentifierQuoteString(), equalTo(IDENTIFIER_QUOTE_STRING)),
              () -> assertThat(schemaAdapterNotes.storesLowerCaseIdentifiers(), equalTo(STORES_LOWER_CASE_IDENTIFIERS)),
              () -> assertThat(schemaAdapterNotes.storesUpperCaseIdentifiers(), equalTo(STORES_UPPER_CASE_IDENTIFIERS)),
              () -> assertThat(schemaAdapterNotes.storesMixedCaseIdentifiers(), equalTo(STORES_MIXED_CASE_IDENTIFIERS)),
              () -> assertThat(schemaAdapterNotes.supportsMixedCaseIdentifiers(),
                    equalTo(SUPPORTS_MIXED_CASE_IDENTIFIERS)),
              () -> assertThat(schemaAdapterNotes.storesLowerCaseQuotedIdentifiers(),
                    equalTo(STORES_LOWER_CASE_QUOTED_IDENTIFIERS)),
              () -> assertThat(schemaAdapterNotes.storesUpperCaseQuotedIdentifiers(),
                    equalTo(STORES_UPPER_CASE_QUOTED_IDENTIFIERS)),
              () -> assertThat(schemaAdapterNotes.storesMixedCaseQuotedIdentifiers(),
                    equalTo(STORES_MIXED_CASE_QUOTED_IDENTIFIERS)),
              () -> assertThat(schemaAdapterNotes.supportsMixedCaseQuotedIdentifiers(),
                    equalTo(SUPPORTS_MIXED_CASE_QUOTED_IDENTIFIERS)),
              () -> assertThat(schemaAdapterNotes.areNullsSortedAtEnd(), equalTo(NULLS_ARE_SORTED_AT_END)),
              () -> assertThat(schemaAdapterNotes.areNullsSortedAtStart(), equalTo(NULLS_ARE_SORTED_AT_START)),
              () -> assertThat(schemaAdapterNotes.areNullsSortedHigh(), equalTo(NULLS_ARE_SORTED_HIGH)),
              () -> assertThat(schemaAdapterNotes.areNullsSortedLow(), equalTo(NULLS_ARE_SORTED_LOW)));
    }
}