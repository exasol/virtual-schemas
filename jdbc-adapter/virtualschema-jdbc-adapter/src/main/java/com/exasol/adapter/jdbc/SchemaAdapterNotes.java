package com.exasol.adapter.jdbc;

import com.exasol.adapter.AdapterException;
import com.exasol.utils.JsonHelper;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 * Holds the schema adapter notes specific to the JDBC Adapter. Also includes functionality to serialize and deserialize.
 */
public class SchemaAdapterNotes {

    // String that this database uses as the separator between a catalog and table name
    private String catalogSeparator;

    // string used to quote SQL identifiers
    private String identifierQuoteString;

    // treats mixed case unquoted SQL identifiers as case insensitive and stores them in lower case.
    private boolean storesLowerCaseIdentifiers;

    // treats mixed case unquoted SQL identifiers as case insensitive and stores them in upper case. TRUE for EXASOL & Oracle
    private boolean storesUpperCaseIdentifiers;

    // treats mixed case unquoted SQL identifiers as case insensitive and stores them in mixed case.
    private boolean storesMixedCaseIdentifiers;

    // treats mixed case unquoted SQL identifiers as case sensitive and as a result stores them in mixed case. TRUE for EXASOL.
    // Seems to be a bug that this is true for EXASOL.
    private boolean supportsMixedCaseIdentifiers;

    // treats mixed case quoted SQL identifiers as case insensitive and stores them in lower case.
    private boolean storesLowerCaseQuotedIdentifiers;

    // treats mixed case quoted SQL identifiers as case insensitive and stores them in upper case.
    private boolean storesUpperCaseQuotedIdentifiers;

    // treats mixed case quoted SQL identifiers as case insensitive and stores them in mixed case. TRUE for Oracle.
    // Oracle has this also true (only difference to EXASOL) which is in conflict with supportsMixedCaseQuotedIdentifiers
    // which states that mixed case quoted identifiers are treated case sensitive.
    private boolean storesMixedCaseQuotedIdentifiers;

    // treats mixed case quoted SQL identifiers as case sensitive and as a result stores them in mixed case. TRUE for EXASOL & Oracle.
    private boolean supportsMixedCaseQuotedIdentifiers;

    // NULL values are sorted at the end regardless of sort order
    private boolean nullsAreSortedAtEnd;

    // NULL values are sorted at the start regardless of sort order
    private boolean nullsAreSortedAtStart;

    // NULL values are sorted high
    private boolean nullsAreSortedHigh;

    // NULL values are sorted low
    private boolean nullsAreSortedLow;

    public SchemaAdapterNotes(String catalogSeparator,
                              String identifierQuoteString,
                              boolean storesLowerCaseIdentifiers,
                              boolean storesUpperCaseIdentifiers,
                              boolean storesMixedCaseIdentifiers,
                              boolean supportsMixedCaseIdentifiers,
                              boolean storesLowerCaseQuotedIdentifiers,
                              boolean storesUpperCaseQuotedIdentifiers,
                              boolean storesMixedCaseQuotedIdentifiers,
                              boolean supportsMixedCaseQuotedIdentifiers,
                              boolean nullsAreSortedAtEnd,
                              boolean nullsAreSortedAtStart,
                              boolean nullsAreSortedHigh,
                              boolean nullsAreSortedLow) {
        this.catalogSeparator = catalogSeparator;
        this.identifierQuoteString = identifierQuoteString;
        this.storesLowerCaseIdentifiers = storesLowerCaseIdentifiers;
        this.storesUpperCaseIdentifiers = storesUpperCaseIdentifiers;
        this.storesMixedCaseIdentifiers = storesMixedCaseIdentifiers;
        this.supportsMixedCaseIdentifiers = supportsMixedCaseIdentifiers;
        this.storesLowerCaseQuotedIdentifiers = storesLowerCaseQuotedIdentifiers;
        this.storesUpperCaseQuotedIdentifiers = storesUpperCaseQuotedIdentifiers;
        this.storesMixedCaseQuotedIdentifiers = storesMixedCaseQuotedIdentifiers;
        this.supportsMixedCaseQuotedIdentifiers = supportsMixedCaseQuotedIdentifiers;
        this.nullsAreSortedAtEnd = nullsAreSortedAtEnd;
        this.nullsAreSortedAtStart = nullsAreSortedAtStart;
        this.nullsAreSortedHigh = nullsAreSortedHigh;
        this.nullsAreSortedLow = nullsAreSortedLow;
    }

    public String getCatalogSeparator() {
        return catalogSeparator;
    }

    public String getIdentifierQuoteString() {
        return identifierQuoteString;
    }

    public boolean isSupportsMixedCaseIdentifiers() {
        return supportsMixedCaseIdentifiers;
    }

    public boolean isSupportsMixedCaseQuotedIdentifiers() {
        return supportsMixedCaseQuotedIdentifiers;
    }

    public boolean isStoresLowerCaseIdentifiers() {
        return storesLowerCaseIdentifiers;
    }

    public boolean isStoresUpperCaseIdentifiers() {
        return storesUpperCaseIdentifiers;
    }

    public boolean isStoresMixedCaseIdentifiers() {
        return storesMixedCaseIdentifiers;
    }

    public boolean isStoresLowerCaseQuotedIdentifiers() {
        return storesLowerCaseQuotedIdentifiers;
    }

    public boolean isStoresUpperCaseQuotedIdentifiers() {
        return storesUpperCaseQuotedIdentifiers;
    }

    public boolean isStoresMixedCaseQuotedIdentifiers() {
        return storesMixedCaseQuotedIdentifiers;
    }

    public boolean isNullsAreSortedAtEnd() {
        return nullsAreSortedAtEnd;
    }

    public boolean isNullsAreSortedAtStart() {
        return nullsAreSortedAtStart;
    }

    public boolean isNullsAreSortedHigh() {
        return nullsAreSortedHigh;
    }

    public boolean isNullsAreSortedLow() {
        return nullsAreSortedLow;
    }

    public static String serialize(SchemaAdapterNotes notes) {
        JsonBuilderFactory factory = JsonHelper.getBuilderFactory();
        JsonObjectBuilder builder = factory.createObjectBuilder()
                .add("catalogSeparator", notes.getCatalogSeparator())
                .add("identifierQuoteString", notes.getIdentifierQuoteString())
                .add("storesLowerCaseIdentifiers", notes.isStoresLowerCaseIdentifiers())
                .add("storesUpperCaseIdentifiers", notes.isStoresUpperCaseIdentifiers())
                .add("storesMixedCaseIdentifiers", notes.isStoresMixedCaseIdentifiers())
                .add("supportsMixedCaseIdentifiers", notes.isSupportsMixedCaseIdentifiers())
                .add("storesLowerCaseQuotedIdentifiers", notes.isStoresLowerCaseQuotedIdentifiers())
                .add("storesUpperCaseQuotedIdentifiers", notes.isStoresUpperCaseQuotedIdentifiers())
                .add("storesMixedCaseQuotedIdentifiers", notes.isStoresMixedCaseQuotedIdentifiers())
                .add("supportsMixedCaseQuotedIdentifiers", notes.isSupportsMixedCaseQuotedIdentifiers())
                .add("nullsAreSortedAtEnd", notes.isNullsAreSortedAtEnd())
                .add("nullsAreSortedAtStart", notes.isNullsAreSortedAtStart())
                .add("nullsAreSortedHigh", notes.isNullsAreSortedHigh())
                .add("nullsAreSortedLow", notes.isNullsAreSortedLow());
        return builder.build().toString();
    }

    public static SchemaAdapterNotes deserialize(String adapterNotes, String schemaName) throws AdapterException {
        if (adapterNotes == null || adapterNotes.isEmpty()) {
            throw new AdapterException("Adapter notes for schema "+schemaName+" are empty or null. Please refresh the virtual schema");
        }
        JsonObject root;
        try {
            root = JsonHelper.getJsonObject(adapterNotes);
        } catch (Exception ex) {
            throw new AdapterException("Could not parse the json which is expected to be stored in the adapter notes of schema "+schemaName+". Please refresh the virtual schema");
        }
        checkKey(root, "catalogSeparator", schemaName);
        checkKey(root, "identifierQuoteString", schemaName);
        checkKey(root, "storesLowerCaseIdentifiers", schemaName);
        checkKey(root, "storesUpperCaseIdentifiers", schemaName);
        checkKey(root, "storesMixedCaseIdentifiers", schemaName);
        checkKey(root, "supportsMixedCaseIdentifiers", schemaName);
        checkKey(root, "storesLowerCaseQuotedIdentifiers", schemaName);
        checkKey(root, "storesUpperCaseQuotedIdentifiers", schemaName);
        checkKey(root, "storesMixedCaseQuotedIdentifiers", schemaName);
        checkKey(root, "supportsMixedCaseQuotedIdentifiers", schemaName);
        checkKey(root, "nullsAreSortedAtEnd", schemaName);
        checkKey(root, "nullsAreSortedAtStart", schemaName);
        checkKey(root, "nullsAreSortedHigh", schemaName);
        checkKey(root, "nullsAreSortedLow", schemaName);
        return new SchemaAdapterNotes(
                root.getString("catalogSeparator"),
                root.getString("identifierQuoteString"),
                root.getBoolean("storesLowerCaseIdentifiers"),
                root.getBoolean("storesUpperCaseIdentifiers"),
                root.getBoolean("storesMixedCaseIdentifiers"),
                root.getBoolean("supportsMixedCaseIdentifiers"),
                root.getBoolean("storesLowerCaseQuotedIdentifiers"),
                root.getBoolean("storesUpperCaseQuotedIdentifiers"),
                root.getBoolean("storesMixedCaseQuotedIdentifiers"),
                root.getBoolean("supportsMixedCaseQuotedIdentifiers"),
                root.getBoolean("nullsAreSortedAtEnd"),
                root.getBoolean("nullsAreSortedAtStart"),
                root.getBoolean("nullsAreSortedHigh"),
                root.getBoolean("nullsAreSortedLow"));
    }

    private static void checkKey(JsonObject root, String key, String schemaName) throws AdapterException {
        if (!root.containsKey(key)) {
            throw new AdapterException("Adapter notes of schema " + schemaName + " don't have the key " + key +". Please refresh the virtual schema");
        }
    }
}
