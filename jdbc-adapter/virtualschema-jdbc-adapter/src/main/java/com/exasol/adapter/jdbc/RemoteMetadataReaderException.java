package com.exasol.adapter.jdbc;

/**
 * This class represents exceptional conditions that occur during attempts to extract schema metadata from a remote data
 * source.
 */
public class RemoteMetadataReaderException extends RuntimeException {
    private static final long serialVersionUID = -3538574076638223017L;

    /**
     * Create a new {@link RemoteMetadataReaderException}.
     *
     * @param message error message
     * @param cause   root cause
     */
    public RemoteMetadataReaderException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Create a new {@link RemoteMetadataReaderException}.
     *
     * @param message error message
     */
    public RemoteMetadataReaderException(final String message) {
        super(message);
    }
}