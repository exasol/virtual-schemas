package com.exasol.adapter.dialects;

public class SqlGenerationVisitorException extends RuntimeException {
    private static final long serialVersionUID = -3119998339625992764L;

    public SqlGenerationVisitorException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
