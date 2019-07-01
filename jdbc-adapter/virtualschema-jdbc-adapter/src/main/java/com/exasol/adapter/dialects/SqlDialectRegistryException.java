package com.exasol.adapter.dialects;

/**
 * This class provides runtime exceptions for the SQL dialects registry.
 */
public class SqlDialectRegistryException extends RuntimeException {
    private static final long serialVersionUID = -5603866366083182805L;

    /**
     * Create a new instance of the {@link SqlDialectRegistryException}.
     *
     * @param message message to be displayed
     */
    public SqlDialectRegistryException(final String message) {
        super(message);
    }

    /**
     * Create a new instance of the {@link SqlDialectRegistryException}.
     *
     * @param message message to be displayed
     * @param cause   root cause
     */
    public SqlDialectRegistryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}