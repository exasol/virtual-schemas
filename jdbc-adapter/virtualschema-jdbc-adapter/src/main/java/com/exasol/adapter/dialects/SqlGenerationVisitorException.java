package com.exasol.adapter.dialects;

/**
 * Represents an exceptional situation while dialects' SQL generation visitors are running.
 */
public class SqlGenerationVisitorException extends RuntimeException {
    private static final long serialVersionUID = -3119998339625992764L;

    /**
     * Creates a new instance of {@link SqlGenerationVisitorException}.
     *
     * @param message error message
     * @param cause   exception's cause
     */
    public SqlGenerationVisitorException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance of {@link SqlGenerationVisitorException}.
     *
     * @param message error message
     */
    public SqlGenerationVisitorException(final String message) {
        super(message);
    }
}
