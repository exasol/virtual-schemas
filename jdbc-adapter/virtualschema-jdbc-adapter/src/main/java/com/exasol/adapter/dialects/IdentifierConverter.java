package com.exasol.adapter.dialects;

import com.exasol.adapter.jdbc.IdentifierCaseHandling;

/**
 * This class represents SQL table and column names converter.
 */
public class IdentifierConverter {
    private final IdentifierCaseHandling unquotedIdentifierHandling;
    private final IdentifierCaseHandling quotedIdentifierHandling;

    /**
     * Create a new instance of {@link IdentifierConverter} class.
     *
     * @param unquotedIdentifierHandling handling for unquoted identifiers
     * @param quotedIdentifierHandling   handling for quoted identifiers
     */
    public IdentifierConverter(final IdentifierCaseHandling unquotedIdentifierHandling,
            final IdentifierCaseHandling quotedIdentifierHandling) {
        this.unquotedIdentifierHandling = unquotedIdentifierHandling;
        this.quotedIdentifierHandling = quotedIdentifierHandling;
    }

    /**
     * Convert identifier according to the handling variables which were sent into the constructor.
     *
     * @param identifier identifier to convert
     * @return converted identifier
     */
    public String convert(final String identifier) {
        if (this.quotedIdentifierHandling == this.unquotedIdentifierHandling) {
            if (this.quotedIdentifierHandling != IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE) {
                return identifier.toUpperCase();
            }
        }
        return identifier;
    }

    /**
     * Get unquoted identifier case handling.
     *
     * @return unquoted identifier case handling
     */
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return this.unquotedIdentifierHandling;
    }

    /**
     * Get quoted identifier case handling.
     *
     * @return quoted identifier case handling
     */
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return this.quotedIdentifierHandling;
    }

    /**
     * Create a new instance of an {@link IdentifierConverter} with the default behavior
     * 
     * @return new instance
     */
    public static IdentifierConverter createDefault() {
        return new IdentifierConverter(IdentifierCaseHandling.INTERPRET_AS_UPPER,
                IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE);
    }
}