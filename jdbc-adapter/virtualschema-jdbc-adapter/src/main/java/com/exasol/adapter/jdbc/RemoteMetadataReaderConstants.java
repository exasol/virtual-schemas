package com.exasol.adapter.jdbc;

import java.util.*;

/**
 * This class contains constants that are relevant for any of the classes involved with reading remote metadata.
 */
public final class RemoteMetadataReaderConstants {
    private RemoteMetadataReaderConstants() {
        // prevent instantiation
    }

    public static final String ANY_CATALOG = null;
    public static final String ANY_SCHEMA = null;
    public static final String ANY_TABLE = "%";
    public static final String ANY_COLUMN = "%";
    public static final String JDBC_TRUE = "yes";
    public static final String JDBC_FALSE = "no";
    public static final Set<String> DEFAULT_SUPPORTED_TABLE_TYPES = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList("TABLE", "VIEW", "SYSTEM TABLE")));
    public static final Set<String> ANY_TABLE_TYPE = Collections.emptySet();
}