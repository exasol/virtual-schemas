package com.exasol.adapter.dialects;

/**
 * This class represents SQL table and column names converter.
 */
public class BaseIdentifierConverter extends AbstractIdentifierConverter {
    /**
     * Create a new instance of {@link IdentifierConverter} class.
     *
     * @param unquotedIdentifierHandling handling for unquoted identifiers
     * @param quotedIdentifierHandling   handling for quoted identifiers
     */
    public BaseIdentifierConverter(final IdentifierCaseHandling unquotedIdentifierHandling,
            final IdentifierCaseHandling quotedIdentifierHandling) {
        super(unquotedIdentifierHandling, quotedIdentifierHandling);
    }

    /**
     * Convert identifier according to the handling variables which were sent into the constructor.
     *
     * @param identifier identifier to convert
     * @return converted identifier
     */
    @Override
    public String convert(final String identifier) {
        if ((this.quotedIdentifierHandling == this.unquotedIdentifierHandling)
                && (this.quotedIdentifierHandling != IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE)) {
            return identifier.toUpperCase();
        } else {
            return identifier;
        }
    }

    /**
     * Create a new instance of an {@link IdentifierConverter} with the default behavior.
     *
     * @return new instance of {@link IdentifierConverter}
     */
    public static IdentifierConverter createDefault() {
        return new BaseIdentifierConverter(IdentifierCaseHandling.INTERPRET_AS_UPPER,
                IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE);
    }
}