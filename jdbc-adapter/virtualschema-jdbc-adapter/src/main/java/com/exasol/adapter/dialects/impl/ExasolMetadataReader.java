package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.jdbc.ColumnMetadataReader;

import java.sql.Connection;

public class ExasolMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link ExasolMetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public ExasolMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new ExasolColumnMetadataReader(this.connection, this.properties);
    }
}
