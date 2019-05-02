package com.exasol.adapter.dialects;

import com.exasol.adapter.jdbc.IdentifierCaseHandling;

/**
 * Common interface for database identifier converters
 */
public interface IdentifierConverter {
    /**
     * Convert identifier according to the handling variables which were sent into the constructor.
     *
     * @param identifier identifier to convert
     * @return converted identifier
     */
    String convert(String identifier);

    /**
     * Get unquoted identifier case handling.
     *
     * @return unquoted identifier case handling
     */
    IdentifierCaseHandling getUnquotedIdentifierHandling();

    /**
     * Get quoted identifier case handling.
     *
     * @return quoted identifier case handling
     */
    IdentifierCaseHandling getQuotedIdentifierHandling();
}