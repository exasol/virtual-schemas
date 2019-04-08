package com.exasol.adapter.dialects.impl;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.jdbc.BaseTableMetadataReader;

/**
 * This class implements a reader for PostgreSQL-specific metadata
 */
public class PostgreSQLMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link PostgreSQLMetadataReader}
     *
     * @param connection connection to the PostgreSQL database
     * @param properties user-defined adapter properties
     */
    public PostgreSQLMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected BaseTableMetadataReader createTableMetadataReader() {
        return new PostgreSQLTableMetadataReader(createColumnMetadataReader(), this.properties);
    }
}