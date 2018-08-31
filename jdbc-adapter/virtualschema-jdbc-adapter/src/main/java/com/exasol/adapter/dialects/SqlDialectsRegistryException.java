package com.exasol.adapter.dialects;

public class SqlDialectsRegistryException extends RuntimeException {
    private static final long serialVersionUID = -5603866366083182805L;

    public SqlDialectsRegistryException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public SqlDialectsRegistryException(final String message) {
        super(message);
    }
}
