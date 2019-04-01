package com.exasol.adapter.jdbc;

import java.util.Objects;

/**
 * Holds the schema adapter notes specific to the JDBC Adapter. Also includes functionality to serialize and
 * deserialize.
 */
public final class SchemaAdapterNotes {
    private final String catalogSeparator;
    private final String identifierQuoteString;
    private final boolean storesLowerCaseIdentifiers;
    private final boolean storesUpperCaseIdentifiers;
    private final boolean storesMixedCaseIdentifiers;
    private final boolean supportsMixedCaseIdentifiers;
    private final boolean storesLowerCaseQuotedIdentifiers;
    private final boolean storesUpperCaseQuotedIdentifiers;
    private final boolean storesMixedCaseQuotedIdentifiers;
    private final boolean supportsMixedCaseQuotedIdentifiers;
    private final boolean nullsAreSortedAtEnd;
    private final boolean nullsAreSortedAtStart;
    private final boolean nullsAreSortedHigh;
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

    /**
     * Create a new builder for {@link SchemaAdapterNotes}
     *
     * @return builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @return string that this database uses as the separator between a catalog and table name.
     */
    public String getCatalogSeparator() {
        return this.catalogSeparator;
    }

    /**
     * @return string that used to quote SQL identifiers.
     */
    public String getIdentifierQuoteString() {
        return this.identifierQuoteString;
    }

    /**
     * @return true if remote database treats mixed case unquoted SQL identifiers as case sensitive
     * and as a result stores them in mixed case.
     */
    public boolean supportsMixedCaseIdentifiers() {
        return this.supportsMixedCaseIdentifiers;
    }

    /**
     * @return true if remote database treats mixed case quoted SQL identifiers as case sensitive
     * and as a result stores them in mixed case. TRUE for EXASOL & Oracle.
     */
    public boolean supportsMixedCaseQuotedIdentifiers() {
        return this.supportsMixedCaseQuotedIdentifiers;
    }

    /**
     * @return true if remote database treats mixed case unquoted SQL identifiers
     * as case insensitive and stores them in lower case.
     */
    public boolean storesLowerCaseIdentifiers() {
        return this.storesLowerCaseIdentifiers;
    }

    /**
     * @return true if remote database treats mixed case unquoted SQL identifiers
     * as case insensitive and stores them in upper case. TRUE for EXASOL & Oracle
     */
    public boolean storesUpperCaseIdentifiers() {
        return this.storesUpperCaseIdentifiers;
    }

    /**
     * @return true if remote database treats mixed case unquoted SQL identifiers
     * as case insensitive and stores them in mixed case.
     */
    public boolean storesMixedCaseIdentifiers() {
        return this.storesMixedCaseIdentifiers;
    }

    /**
     * @return true if remote database treats mixed case quoted SQL identifiers as case insensitive
     * and stores them in lower case.
     */
    public boolean storesLowerCaseQuotedIdentifiers() {
        return this.storesLowerCaseQuotedIdentifiers;
    }

    /**
     * @return true if remote database treats mixed case quoted SQL identifiers as case insensitive
     * and stores them in upper case.
     */
    public boolean storesUpperCaseQuotedIdentifiers() {
        return this.storesUpperCaseQuotedIdentifiers;
    }

    /**
     * @return true if remote database treats mixed case quoted SQL identifiers as case insensitive
     * and stores them in mixed case. TRUE for Oracle.
     */
    public boolean storesMixedCaseQuotedIdentifiers() {
        return this.storesMixedCaseQuotedIdentifiers;
    }

    /**
     * @return true if NULL values are sorted at the end regardless of sort order.
     */
    public boolean nullsAreSortedAtEnd() {
        return this.nullsAreSortedAtEnd;
    }

    /**
     * @return true if NULL values are sorted at the start regardless of sort order.
     */
    public boolean nullsAreSortedAtStart() {
        return this.nullsAreSortedAtStart;
    }

    /**
     * @return true if NULL values are sorted high.
     */
    public boolean nullsAreSortedHigh() {
        return this.nullsAreSortedHigh;
    }

    /**
     * @return true if NULL values are sorted low.
     */
    public boolean nullsAreSortedLow() {
        return this.nullsAreSortedLow;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SchemaAdapterNotes that = (SchemaAdapterNotes) o;
        return this.storesLowerCaseIdentifiers == that.storesLowerCaseIdentifiers
              && this.storesUpperCaseIdentifiers == that.storesUpperCaseIdentifiers
              && this.storesMixedCaseIdentifiers == that.storesMixedCaseIdentifiers
              && this.supportsMixedCaseIdentifiers == that.supportsMixedCaseIdentifiers
              && this.storesLowerCaseQuotedIdentifiers == that.storesLowerCaseQuotedIdentifiers
              && this.storesUpperCaseQuotedIdentifiers == that.storesUpperCaseQuotedIdentifiers
              && this.storesMixedCaseQuotedIdentifiers == that.storesMixedCaseQuotedIdentifiers
              && this.supportsMixedCaseQuotedIdentifiers == that.supportsMixedCaseQuotedIdentifiers
              && this.nullsAreSortedAtEnd == that.nullsAreSortedAtEnd
              && this.nullsAreSortedAtStart == that.nullsAreSortedAtStart
              && this.nullsAreSortedHigh == that.nullsAreSortedHigh && this.nullsAreSortedLow == that.nullsAreSortedLow
              && Objects.equals(this.catalogSeparator, that.catalogSeparator) && Objects
              .equals(this.identifierQuoteString, that.identifierQuoteString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.catalogSeparator, this.identifierQuoteString, this.storesLowerCaseIdentifiers,
              this.storesUpperCaseIdentifiers, this.storesMixedCaseIdentifiers, this.supportsMixedCaseIdentifiers,
              this.storesLowerCaseQuotedIdentifiers, this.storesUpperCaseQuotedIdentifiers,
              this.storesMixedCaseQuotedIdentifiers, this.supportsMixedCaseQuotedIdentifiers, this.nullsAreSortedAtEnd,
              this.nullsAreSortedAtStart, this.nullsAreSortedHigh, this.nullsAreSortedLow);
    }

    /**
     * Builder for {@link SchemaAdapterNotes}
     */
    public static class Builder {
        private String catalogSeparator = ".";
        private String identifierQuoteString = "\"";
        private boolean storesLowerCaseIdentifiers = false;
        private boolean storesUpperCaseIdentifiers = false;
        private boolean storesMixedCaseIdentifiers = false;
        private boolean supportsMixedCaseIdentifiers = false;
        private boolean storesLowerCaseQuotedIdentifiers = false;
        private boolean storesUpperCaseQuotedIdentifiers = false;
        private boolean storesMixedCaseQuotedIdentifiers = false;
        private boolean supportsMixedCaseQuotedIdentifiers = false;
        private boolean nullsAreSortedAtEnd = false;
        private boolean nullsAreSortedAtStart = false;
        private boolean nullsAreSortedHigh = false;
        private boolean nullsAreSortedLow = false;

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
