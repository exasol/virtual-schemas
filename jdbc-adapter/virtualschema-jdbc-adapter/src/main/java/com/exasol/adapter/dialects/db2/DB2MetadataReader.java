package com.exasol.adapter.dialects.db2;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.jdbc.ColumnMetadataReader;

import java.sql.Connection;

/**
 * This class reads and DB2-specific database metadata
 */
public class DB2MetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link DB2MetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public DB2MetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new DB2ColumnMetadataReader(this.connection, this.properties);
    }
}
