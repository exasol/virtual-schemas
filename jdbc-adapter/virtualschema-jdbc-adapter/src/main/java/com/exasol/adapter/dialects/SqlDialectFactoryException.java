package com.exasol.adapter.dialects;

/**
 * This class represents exceptional conditions that occur during creation of SQL dialects.
 */
public class SqlDialectFactoryException extends RuntimeException {
    private static final long serialVersionUID = 5317697617684968131L;

    /**
     * Create a new {@link SqlDialectFactoryException}
     *
     * @param message error message
     * @param cause   root cause
     */
    public SqlDialectFactoryException(final String message, final Throwable cause) {
        super(message, cause);
    }
}