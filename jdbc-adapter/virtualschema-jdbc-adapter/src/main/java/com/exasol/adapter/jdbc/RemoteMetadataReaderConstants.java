package com.exasol.adapter.jdbc;

/**
 * This class contains constants that are relevant for any of the classes involved with reading remote metadata
 */
public final class RemoteMetadataReaderConstants {
    private RemoteMetadataReaderConstants() {
        // prevent instantiation
    }

    public static final String ANY_CATALOG = null;
    public static final String ANY_SCHEMA = null;
    public static final String ANY_TABLE = "%";
    public static final String[] ANY_TYPE = null;
    public static final String ANY_COLUMN = "%";
    public static final String JDBC_TRUE = "yes";
    public static final String JDBC_FALSE = "no";
}