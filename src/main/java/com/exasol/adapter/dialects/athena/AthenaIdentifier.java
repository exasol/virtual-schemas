package com.exasol.adapter.dialects.athena;

import java.util.Objects;

import com.exasol.db.Identifier;

/**
 * Represents an identifier in the Athena database.
 */
public class AthenaIdentifier implements Identifier {
    private final String id;

    private AthenaIdentifier(final String id) {
        this.id = id;
    }

    /**
     * Get the quoted identifier as a {@link String}.
     *
     * @return quoted identifier
     */
    @Override
    public String quote() {
        if (this.id.startsWith("_")) {
            return quoteWithBackticks(this.id);
        } else {
            return quoteWithDoubleQuotes(this.id);
        }
    }

    private String quoteWithBackticks(final String identifier) {
        return "`" + identifier + "`";
    }

    private String quoteWithDoubleQuotes(final String identifier) {
        return "\"" + identifier + "\"";
    }

    /**
     * Create a new {@link AthenaIdentifier}.
     *
     * @param id the identifier as {@link String}
     * @return new {@link AthenaIdentifier} instance
     */
    public static AthenaIdentifier of(final String id) {
        if (validate(id)) {
            return new AthenaIdentifier(id);
        } else {
            throw new AssertionError("E-ID-2: Unable to create identifier \"" + id //
                    + "\" because it contains illegal characters." //
                    + " For information about valid identifiers, please refer to" //
                    + " https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html");
        }
    }

    private static boolean validate(final String id) {
        if ((id == null) || id.isEmpty()) {
            return false;
        }
        for (int i = 0; i < id.length(); ++i) {
            if (!validateCharacter(id.codePointAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean validateCharacter(final int codePoint) {
        return codePoint == '_' || Character.isLetter(codePoint) || Character.isDigit(codePoint);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AthenaIdentifier)) {
            return false;
        }
        final AthenaIdentifier that = (AthenaIdentifier) o;
        return Objects.equals(this.id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }
}
