package com.exasol.adapter.jdbc;

/**
 * This class provides runtime exceptions for metadata retrieval.
 */
public class RetrieveMetadataException extends RuntimeException {
    private static final long serialVersionUID = -2971883064471515520L;

    /**
     * Create a new instance of the {@link RetrieveMetadataException}
     *
     * @param message message to be displayed
     * @param cause   root cause
     */
    public RetrieveMetadataException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
