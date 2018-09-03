package com.exasol.adapter.dialects;

/**
 * Runtime exception for SQL dialect loading errors.
 */
public class SqlDialectLoaderException extends RuntimeException {
    private static final long serialVersionUID = 763181882639544730L;

    public SqlDialectLoaderException(final String string, final Throwable cause) {
        super(string, cause);
    }
}
