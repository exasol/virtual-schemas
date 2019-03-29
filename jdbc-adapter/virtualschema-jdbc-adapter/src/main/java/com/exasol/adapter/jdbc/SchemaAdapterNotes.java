package com.exasol.adapter.jdbc;

import com.exasol.adapter.AdapterException;
import com.exasol.utils.JsonHelper;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

/**
 * Holds the schema adapter notes specific to the JDBC Adapter. Also includes functionality to serialize and
 * deserialize.
 */
public class SchemaAdapterNotes {
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

    private final String catalogSeparator;
    private final String identifierQuoteString;

    // treats mixed case unquoted SQL identifiers as case insensitive and stores them in lower case.
    private final boolean storesLowerCaseIdentifiers;

    // treats mixed case unquoted SQL identifiers as case insensitive and stores them in upper case. TRUE for EXASOL
    // & Oracle
    private final boolean storesUpperCaseIdentifiers;

    // treats mixed case unquoted SQL identifiers as case insensitive and stores them in mixed case.
    private final boolean storesMixedCaseIdentifiers;

    // treats mixed case unquoted SQL identifiers as case sensitive and as a result stores them in mixed case. TRUE
    // for EXASOL.
    // Seems to be a bug that this is true for EXASOL.
    private final boolean supportsMixedCaseIdentifiers;

    // treats mixed case quoted SQL identifiers as case insensitive and stores them in lower case.
    private final boolean storesLowerCaseQuotedIdentifiers;

    // treats mixed case quoted SQL identifiers as case insensitive and stores them in upper case.
    private final boolean storesUpperCaseQuotedIdentifiers;

    // treats mixed case quoted SQL identifiers as case insensitive and stores them in mixed case. TRUE for Oracle.
    // Oracle has this also true (only difference to EXASOL) which is in conflict with
    // supportsMixedCaseQuotedIdentifiers
    // which states that mixed case quoted identifiers are treated case sensitive.
    private final boolean storesMixedCaseQuotedIdentifiers;

    // treats mixed case quoted SQL identifiers as case sensitive and as a result stores them in mixed case. TRUE for
    // EXASOL & Oracle.
    private final boolean supportsMixedCaseQuotedIdentifiers;

    // NULL values are sorted at the end regardless of sort order
    private final boolean nullsAreSortedAtEnd;

    // NULL values are sorted at the start regardless of sort order
    private final boolean nullsAreSortedAtStart;

    // NULL values are sorted high
    private final boolean nullsAreSortedHigh;

    // NULL values are sorted low
    private final boolean nullsAreSortedLow;

    private SchemaAdapterNotes(final Builder builder) {
        this.catalogSeparator = builder.catalogSeparator;
        this.identifierQuoteString = builder.identifierQuoteString;
        this.storesLowerCaseIdentifiers = builder.storesLowerCaseIdentifiers;
        this.storesUpperCaseIdentifiers = builder.storesUpperCaseIdentifiers;
        this.storesMixedCaseIdentifiers = builder.storesMixedCaseIdentifiers;
        this.supportsMixedCaseIdentifiers = builder.supportsMixedCaseIdentifiers;
        this.storesLowerCaseQuotedIdentifiers = builder.storesLowerCaseQuotedIdentifiers;
        this.storesUpperCaseQuotedIdentifiers = builder.storesUpperCaseQuotedIdentifiers;
        this.storesMixedCaseQuotedIdentifiers = builder.storesMixedCaseQuotedIdentifiers;
        this.supportsMixedCaseQuotedIdentifiers = builder.supportsMixedCaseQuotedIdentifiers;
        this.nullsAreSortedAtEnd = builder.nullsAreSortedAtEnd;
        this.nullsAreSortedAtStart = builder.nullsAreSortedAtStart;
        this.nullsAreSortedHigh = builder.nullsAreSortedHigh;
        this.nullsAreSortedLow = builder.nullsAreSortedLow;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * @return string that this database uses as the separator between a catalog and table name
     */
    public String getCatalogSeparator() {
        return this.catalogSeparator;
    }

    /**
     * @return string used to quote SQL identifiers
     */
    public String getIdentifierQuoteString() {
        return this.identifierQuoteString;
    }

    public boolean isSupportsMixedCaseIdentifiers() {
        return this.supportsMixedCaseIdentifiers;
    }

    public boolean isSupportsMixedCaseQuotedIdentifiers() {
        return this.supportsMixedCaseQuotedIdentifiers;
    }

    public boolean isStoresLowerCaseIdentifiers() {
        return this.storesLowerCaseIdentifiers;
    }

    public boolean isStoresUpperCaseIdentifiers() {
        return this.storesUpperCaseIdentifiers;
    }

    public boolean isStoresMixedCaseIdentifiers() {
        return this.storesMixedCaseIdentifiers;
    }

    public boolean isStoresLowerCaseQuotedIdentifiers() {
        return this.storesLowerCaseQuotedIdentifiers;
    }

    public boolean isStoresUpperCaseQuotedIdentifiers() {
        return this.storesUpperCaseQuotedIdentifiers;
    }

    public boolean isStoresMixedCaseQuotedIdentifiers() {
        return this.storesMixedCaseQuotedIdentifiers;
    }

    public boolean isNullsAreSortedAtEnd() {
        return this.nullsAreSortedAtEnd;
    }

    public boolean isNullsAreSortedAtStart() {
        return this.nullsAreSortedAtStart;
    }

    public boolean isNullsAreSortedHigh() {
        return this.nullsAreSortedHigh;
    }

    public boolean isNullsAreSortedLow() {
        return this.nullsAreSortedLow;
    }

    public static String serialize(final SchemaAdapterNotes notes) {
        final JsonBuilderFactory factory = JsonHelper.getBuilderFactory();
        final JsonObjectBuilder builder =
              factory.createObjectBuilder().add(CATALOG_SEPARATOR, notes.getCatalogSeparator())
                    .add(IDENTIFIER_QUOTE_STRING, notes.getIdentifierQuoteString())
                    .add(STORES_LOWER_CASE_IDENTIFIERS, notes.isStoresLowerCaseIdentifiers())
                    .add(STORES_UPPER_CASE_IDENTIFIERS, notes.isStoresUpperCaseIdentifiers())
                    .add(STORES_MIXED_CASE_IDENTIFIERS, notes.isStoresMixedCaseIdentifiers())
                    .add(SUPPORTS_MIXED_CASE_IDENTIFIERS, notes.isSupportsMixedCaseIdentifiers())
                    .add(STORES_LOWER_CASE_QUOTED_IDENTIFIERS, notes.isStoresLowerCaseQuotedIdentifiers())
                    .add(STORES_UPPER_CASE_QUOTED_IDENTIFIERS, notes.isStoresUpperCaseQuotedIdentifiers())
                    .add(STORES_MIXED_CASE_QUOTED_IDENTIFIERS, notes.isStoresMixedCaseQuotedIdentifiers())
                    .add(SUPPORTS_MIXED_CASE_QUOTED_IDENTIFIERS, notes.isSupportsMixedCaseQuotedIdentifiers())
                    .add(NULLS_ARE_SORTED_AT_END, notes.isNullsAreSortedAtEnd())
                    .add(NULLS_ARE_SORTED_AT_START, notes.isNullsAreSortedAtStart())
                    .add(NULLS_ARE_SORTED_HIGH, notes.isNullsAreSortedHigh())
                    .add(NULLS_ARE_SORTED_LOW, notes.isNullsAreSortedLow());
        return builder.build().toString();
    }

    public static SchemaAdapterNotes deserialize(final String adapterNotes, final String schemaName)
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
                  "Could not parse the json which is expected to be stored in the adapter notes of schema " +
                        schemaName + ". Please refresh the virtual schema");
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
            throw new AdapterException("Adapter notes of schema " + schemaName + " don't have the key " + key +
                  ". Please refresh the virtual schema");
        }
    }

    /**
     * Builder for {@link SchemaAdapterNotes}
     */
    public static class Builder {
        private String catalogSeparator;
        private String identifierQuoteString;
        private boolean storesLowerCaseIdentifiers;
        private boolean storesUpperCaseIdentifiers;
        private boolean storesMixedCaseIdentifiers;
        private boolean supportsMixedCaseIdentifiers;
        private boolean storesLowerCaseQuotedIdentifiers;
        private boolean storesUpperCaseQuotedIdentifiers;
        private boolean storesMixedCaseQuotedIdentifiers;
        private boolean supportsMixedCaseQuotedIdentifiers;
        private boolean nullsAreSortedAtEnd;
        private boolean nullsAreSortedAtStart;
        private boolean nullsAreSortedHigh;
        private boolean nullsAreSortedLow;

        /**
         * Set the catalog separator
         *
         * @param catalogSeparator catalog separator
         * @return builder instance for fluent programming
         */
        public Builder catalogSeparator(final String catalogSeparator) {
            this.catalogSeparator = catalogSeparator;
            return this;
        }

        /**
         * Set the identifier quote string
         *
         * @param identifierQuoteString identifier quote string
         * @return builder instance for fluent programming
         */
        public Builder identifierQuoteString(final String identifierQuoteString) {
            this.identifierQuoteString = identifierQuoteString;
            return this;
        }

        /**
         * Set true if schema stores lower case identifiers
         *
         * @param storesLowerCaseIdentifiers true if stores lower case identifiers
         * @return builder instance for fluent programming
         */
        public Builder storesLowerCaseIdentifiers(final boolean storesLowerCaseIdentifiers) {
            this.storesLowerCaseIdentifiers = storesLowerCaseIdentifiers;
            return this;
        }

        /**
         * Set true if stores upper case identifiers
         *
         * @param storesUpperCaseIdentifiers true if stores upper case identifiers
         * @return builder instance for fluent programming
         */
        public Builder storesUpperCaseIdentifiers(final boolean storesUpperCaseIdentifiers) {
            this.storesUpperCaseIdentifiers = storesUpperCaseIdentifiers;
            return this;
        }

        /**
         * Set true if stores mixed case identifiers
         *
         * @param storesMixedCaseIdentifiers true if stores mixed case identifiers
         * @return builder instance for fluent programming
         */
        public Builder storesMixedCaseIdentifiers(final boolean storesMixedCaseIdentifiers) {
            this.storesMixedCaseIdentifiers = storesMixedCaseIdentifiers;
            return this;
        }

        /**
         * Set true if supports mixed case identifiers
         *
         * @param supportsMixedCaseIdentifiers true if supports mixed case identifiers
         * @return builder instance for fluent programming
         */
        public Builder supportsMixedCaseIdentifiers(final boolean supportsMixedCaseIdentifiers) {
            this.supportsMixedCaseIdentifiers = supportsMixedCaseIdentifiers;
            return this;
        }

        /**
         * Set true if stores lower case quoted identifiers
         *
         * @param storesLowerCaseQuotedIdentifiers true if stores lower case quoted identifiers
         * @return builder instance for fluent programming
         */
        public Builder storesLowerCaseQuotedIdentifiers(final boolean storesLowerCaseQuotedIdentifiers) {
            this.storesLowerCaseQuotedIdentifiers = storesLowerCaseQuotedIdentifiers;
            return this;
        }

        /**
         * Set true if stores upper case quoted identifiers
         *
         * @param storesUpperCaseQuotedIdentifiers true if stores upper case quoted identifiers
         * @return builder instance for fluent programming
         */
        public Builder storesUpperCaseQuotedIdentifiers(final boolean storesUpperCaseQuotedIdentifiers) {
            this.storesUpperCaseQuotedIdentifiers = storesUpperCaseQuotedIdentifiers;
            return this;
        }

        /**
         * Set true if stores mixed case quoted identifiers
         *
         * @param storesMixedCaseQuotedIdentifiers true if stores mixed case quoted identifiers
         * @return builder instance for fluent programming
         */
        public Builder storesMixedCaseQuotedIdentifiers(final boolean storesMixedCaseQuotedIdentifiers) {
            this.storesMixedCaseQuotedIdentifiers = storesMixedCaseQuotedIdentifiers;
            return this;
        }

        /**
         * Set true if supports mixed case quoted identifiers
         *
         * @param supportsMixedCaseQuotedIdentifiers true if supports mixed case quoted identifiers
         * @return builder instance for fluent programming
         */
        public Builder supportsMixedCaseQuotedIdentifiers(final boolean supportsMixedCaseQuotedIdentifiers) {
            this.supportsMixedCaseQuotedIdentifiers = supportsMixedCaseQuotedIdentifiers;
            return this;
        }

        /**
         * Set true if nulls are sorted at end
         *
         * @param nullsAreSortedAtEnd true if nulls are sorted at end
         * @return builder instance for fluent programming
         */
        public Builder nullsAreSortedAtEnd(final boolean nullsAreSortedAtEnd) {
            this.nullsAreSortedAtEnd = nullsAreSortedAtEnd;
            return this;
        }

        /**
         * Set true if nulls are sorted at start
         *
         * @param nullsAreSortedAtStart true if nulls are sorted at start
         * @return builder instance for fluent programming
         */
        public Builder nullsAreSortedAtStart(final boolean nullsAreSortedAtStart) {
            this.nullsAreSortedAtStart = nullsAreSortedAtStart;
            return this;
        }

        /**
         * Set true if nulls are sorted high
         *
         * @param nullsAreSortedHigh true if nulls are sorted high
         * @return builder instance for fluent programming
         */
        public Builder nullsAreSortedHigh(final boolean nullsAreSortedHigh) {
            this.nullsAreSortedHigh = nullsAreSortedHigh;
            return this;
        }

        /**
         * Set true if nulls are sorted low
         *
         * @param nullsAreSortedLow true if nulls are sorted low
         * @return builder instance for fluent programming
         */
        public Builder nullsAreSortedLow(final boolean nullsAreSortedLow) {
            this.nullsAreSortedLow = nullsAreSortedLow;
            return this;
        }

        public SchemaAdapterNotes build() {
            return new SchemaAdapterNotes(this);
        }
    }
}
