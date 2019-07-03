package com.exasol.adapter.dialects;

/**
 * This class represents exceptional conditions that occur during creation of SQL dialects.
 */
public class SqlDialectFactoryException extends RuntimeException {
    private static final long serialVersionUID = 8924159119956437193L;

    /**
     * Create a new {@link SqlDialectFactoryException}.
     *
     * @param message error message
     */
    public SqlDialectFactoryException(final String message) {
        super(message);
    }

    /**
     * Create a new {@link SqlDialectFactoryException}.
     *
     * @param message error message
     * @param cause   root cause
     */
    public SqlDialectFactoryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}