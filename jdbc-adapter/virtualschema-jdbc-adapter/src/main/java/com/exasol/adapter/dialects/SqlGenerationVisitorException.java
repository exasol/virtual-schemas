package com.exasol.adapter.dialects;

public class SqlGenerationVisitorException extends RuntimeException {
    public SqlGenerationVisitorException(final String message, final Throwable cause) {
        super(message,cause);
    }
}
