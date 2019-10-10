package com.exasol.adapter.dialects;

/**
 * Common interface for database identifier converters.
 */
public interface IdentifierConverter {
    /**
     * Convert identifier according to the handling variables which were sent into the constructor.
     *
     * @param identifier identifier to convert
     * @return converted identifier
     */
    public String convert(String identifier);

    /**
     * Get unquoted identifier case handling.
     *
     * @return unquoted identifier case handling
     */
    public IdentifierCaseHandling getUnquotedIdentifierHandling();

    /**
     * Get quoted identifier case handling.
     *
     * @return quoted identifier case handling
     */
    public IdentifierCaseHandling getQuotedIdentifierHandling();
}