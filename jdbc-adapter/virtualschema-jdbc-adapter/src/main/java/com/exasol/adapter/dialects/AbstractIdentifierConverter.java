package com.exasol.adapter.dialects;

/**
 * Abstract base class for all identifier converters.
 */
public abstract class AbstractIdentifierConverter implements IdentifierConverter {
    protected final IdentifierCaseHandling unquotedIdentifierHandling;
    protected final IdentifierCaseHandling quotedIdentifierHandling;

    /**
     * Create a new instance of an {@link AbstractIdentifierConverter} derived class.
     *
     * @param unquotedIdentifierHandling handling for unquoted identifiers
     * @param quotedIdentifierHandling   handling for quoted identifiers
     */
    public AbstractIdentifierConverter(final IdentifierCaseHandling unquotedIdentifierHandling,
            final IdentifierCaseHandling quotedIdentifierHandling) {
        this.unquotedIdentifierHandling = unquotedIdentifierHandling;
        this.quotedIdentifierHandling = quotedIdentifierHandling;
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return this.unquotedIdentifierHandling;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return this.quotedIdentifierHandling;
    }
}