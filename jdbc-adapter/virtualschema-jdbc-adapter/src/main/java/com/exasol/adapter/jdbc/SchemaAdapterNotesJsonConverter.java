package com.exasol.adapter.jdbc;

import com.exasol.adapter.AdapterException;
import com.exasol.utils.JsonHelper;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 * Converts schema adapter Notes into JSON format and back
 */
public final class SchemaAdapterNotesJsonConverter {
    private static final String CATALOG_SEPARATOR = "catalogSeparator";
    private static final String IDENTIFIER_QUOTE_STRING = "identifierQuoteString";
    private static final String STORES_LOWER_CASE_IDENTIFIERS = "storesLowerCaseIdentifiers";
    private static final String STORES_UPPER_CASE_IDENTIFIERS = "storesUpperCaseIdentifiers";
    private static final String STORES_MIXED_CASE_IDENTIFIERS = "storesMixedCaseIdentifiers";
    private static final String SUPPORTS_MIXED_CASE_IDENTIFIERS = "supportsMixedCaseIdentifiers";
    private static final String STORES_LOWER_CASE_QUOTED_IDENTIFIERS = "storesLowerCaseQuotedIdentifiers";
    private static final String STORES_UPPER_CASE_QUOTED_IDENTIFIERS = "storesUpperCaseQuotedIdentifiers";
    private static final String STORES_MIXED_CASE_QUOTED_IDENTIFIERS = "storesMixedCaseQuotedIdentifiers";
    private static final String SUPPORTS_MIXED_CASE_QUOTED_IDENTIFIERS = "supportsMixedCaseQuotedIdentifiers";
    private static final String NULLS_ARE_SORTED_AT_END = "nullsAreSortedAtEnd";
    private static final String NULLS_ARE_SORTED_AT_START = "nullsAreSortedAtStart";
    private static final String NULLS_ARE_SORTED_HIGH = "nullsAreSortedHigh";
    private static final String NULLS_ARE_SORTED_LOW = "nullsAreSortedLow";

    public static SchemaAdapterNotesJsonConverter getInstance() {
        return new SchemaAdapterNotesJsonConverter();
    }

    private SchemaAdapterNotesJsonConverter(){
    }


    public String convertToJson(final SchemaAdapterNotes notes) {
        final JsonBuilderFactory factory = JsonHelper.getBuilderFactory();
        final JsonObjectBuilder builder =
              factory.createObjectBuilder().add(CATALOG_SEPARATOR, notes.getCatalogSeparator())
                    .add(IDENTIFIER_QUOTE_STRING, notes.getIdentifierQuoteString())
                    .add(STORES_LOWER_CASE_IDENTIFIERS, notes.storesLowerCaseIdentifiers())
                    .add(STORES_UPPER_CASE_IDENTIFIERS, notes.storesUpperCaseIdentifiers())
                    .add(STORES_MIXED_CASE_IDENTIFIERS, notes.storesMixedCaseIdentifiers())
                    .add(SUPPORTS_MIXED_CASE_IDENTIFIERS, notes.supportsMixedCaseIdentifiers())
                    .add(STORES_LOWER_CASE_QUOTED_IDENTIFIERS, notes.storesLowerCaseQuotedIdentifiers())
                    .add(STORES_UPPER_CASE_QUOTED_IDENTIFIERS, notes.storesUpperCaseQuotedIdentifiers())
                    .add(STORES_MIXED_CASE_QUOTED_IDENTIFIERS, notes.storesMixedCaseQuotedIdentifiers())
                    .add(SUPPORTS_MIXED_CASE_QUOTED_IDENTIFIERS, notes.supportsMixedCaseQuotedIdentifiers())
                    .add(NULLS_ARE_SORTED_AT_END, notes.nullsAreSortedAtEnd())
                    .add(NULLS_ARE_SORTED_AT_START, notes.nullsAreSortedAtStart())
                    .add(NULLS_ARE_SORTED_HIGH, notes.nullsAreSortedHigh())
                    .add(NULLS_ARE_SORTED_LOW, notes.nullsAreSortedLow());
        return builder.build().toString();
    }

    public SchemaAdapterNotes convertFromJsonToSchemaAdapterNotes(final String adapterNotes, final String schemaName)
          throws AdapterException {
        if (adapterNotes == null || adapterNotes.isEmpty()) {
            throw new AdapterException(
                  "Adapter notes for schema " + schemaName + " are empty or null. Please refresh the virtual schema");
        }
        final JsonObject root;
        try {
            root = JsonHelper.getJsonObject(adapterNotes);
        } catch (final Exception ex) {
            throw new AdapterException(
                  "Could not parse the json which is expected to be stored in the adapter notes of schema " + schemaName
                        + ". Please refresh the virtual schema");
        }
        checkKey(root, CATALOG_SEPARATOR, schemaName);
        checkKey(root, IDENTIFIER_QUOTE_STRING, schemaName);
        checkKey(root, STORES_LOWER_CASE_IDENTIFIERS, schemaName);
        checkKey(root, STORES_UPPER_CASE_IDENTIFIERS, schemaName);
        checkKey(root, STORES_MIXED_CASE_IDENTIFIERS, schemaName);
        checkKey(root, SUPPORTS_MIXED_CASE_IDENTIFIERS, schemaName);
        checkKey(root, STORES_LOWER_CASE_QUOTED_IDENTIFIERS, schemaName);
        checkKey(root, STORES_UPPER_CASE_QUOTED_IDENTIFIERS, schemaName);
        checkKey(root, STORES_MIXED_CASE_QUOTED_IDENTIFIERS, schemaName);
        checkKey(root, SUPPORTS_MIXED_CASE_QUOTED_IDENTIFIERS, schemaName);
        checkKey(root, NULLS_ARE_SORTED_AT_END, schemaName);
        checkKey(root, NULLS_ARE_SORTED_AT_START, schemaName);
        checkKey(root, NULLS_ARE_SORTED_HIGH, schemaName);
        checkKey(root, NULLS_ARE_SORTED_LOW, schemaName);
        return SchemaAdapterNotes.builder() //
              .catalogSeparator(root.getString(CATALOG_SEPARATOR)) //
              .identifierQuoteString(root.getString(IDENTIFIER_QUOTE_STRING)) //
              .storesLowerCaseIdentifiers(root.getBoolean(STORES_LOWER_CASE_IDENTIFIERS)) //
              .storesUpperCaseIdentifiers(root.getBoolean(STORES_UPPER_CASE_IDENTIFIERS)) //
              .storesMixedCaseIdentifiers(root.getBoolean(STORES_MIXED_CASE_IDENTIFIERS)) //
              .supportsMixedCaseIdentifiers(root.getBoolean(SUPPORTS_MIXED_CASE_IDENTIFIERS)) //
              .storesLowerCaseQuotedIdentifiers(root.getBoolean(STORES_LOWER_CASE_QUOTED_IDENTIFIERS)) //
              .storesUpperCaseQuotedIdentifiers(root.getBoolean(STORES_UPPER_CASE_QUOTED_IDENTIFIERS)) //
              .storesMixedCaseQuotedIdentifiers(root.getBoolean(STORES_MIXED_CASE_QUOTED_IDENTIFIERS)) //
              .supportsMixedCaseQuotedIdentifiers(root.getBoolean(SUPPORTS_MIXED_CASE_QUOTED_IDENTIFIERS))//
              .nullsAreSortedAtEnd(root.getBoolean(NULLS_ARE_SORTED_AT_END)) //
              .nullsAreSortedAtStart(root.getBoolean(NULLS_ARE_SORTED_AT_START)) //
              .nullsAreSortedHigh(root.getBoolean(NULLS_ARE_SORTED_HIGH)) //
              .nullsAreSortedLow(root.getBoolean(NULLS_ARE_SORTED_LOW)) //
              .build();
    }

    private static void checkKey(final JsonObject root, final String key, final String schemaName)
          throws AdapterException {
        if (!root.containsKey(key)) {
            throw new AdapterException("Adapter notes of schema " + schemaName + " don't have the key " + key
                  + ". Please refresh the virtual schema");
        }
    }
}
