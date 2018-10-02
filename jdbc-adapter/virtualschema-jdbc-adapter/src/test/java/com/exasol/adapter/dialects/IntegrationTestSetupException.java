package com.exasol.adapter.dialects;

/**
 * Runtime exceptions for integration test setup
 */
public class IntegrationTestSetupException extends RuntimeException {
    private static final long serialVersionUID = -6106780352553599816L;

    /**
     * Create a new instance of an {@link IntegrationTestSetupException}
     *
     * @param message error message
     */
    public IntegrationTestSetupException(final String message) {
        super(message);
    }

    /**
     * Create a new instance of an {@link IntegrationTestSetupException}
     *
     * @param message error message
     * @param cause   root cause
     */
    public IntegrationTestSetupException(final String message, final Throwable cause) {
        super(message, cause);
    }
}