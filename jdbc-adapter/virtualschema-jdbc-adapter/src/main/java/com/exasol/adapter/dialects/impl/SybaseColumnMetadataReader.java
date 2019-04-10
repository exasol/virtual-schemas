package com.exasol.adapter.dialects.impl;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;

/**
 * This class implements a Sybase-specific column metadata reader
 */
public class SybaseColumnMetadataReader extends BaseColumnMetadataReader {
    /**
     * Create a new instance of a {@link SybaseColumnMetadataReader}
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     */
    public SybaseColumnMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }
}