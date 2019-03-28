package com.exasol.adapter.jdbc;

/**
 * This class represents exceptional conditions that occur during attempts to extract schema metadata from a remote data
 * source.
 */
public class RemoteMetadataReaderException extends Exception {

    public RemoteMetadataReaderException(final String message, final Throwable cause) {
        super(message, cause);
    }
}