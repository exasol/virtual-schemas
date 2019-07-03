package com.exasol.adapter.jdbc;

/**
 * Exception for remote connection problems.
 */
public class RemoteConnectionException extends RuntimeException {
    private static final long serialVersionUID = 550162141981742445L;

    /**
     * Create a new instance of a {@link RemoteConnectionException}.
     *
     * @param message error message
     * @param cause   cause of the exception
     */
    public RemoteConnectionException(final String message, final Throwable cause) {
        super(message, cause);
    }
}