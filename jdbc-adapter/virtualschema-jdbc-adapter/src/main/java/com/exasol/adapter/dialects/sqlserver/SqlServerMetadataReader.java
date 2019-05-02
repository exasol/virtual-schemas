package com.exasol.adapter.dialects.sqlserver;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.jdbc.ColumnMetadataReader;

/**
 * This class implement a SQLServer-specific metadata reader
 */
public class SqlServerMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link SqlServerMetadataReader}
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     */
    public SqlServerMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new SqlServerColumnMetadataReader(this.connection, this.properties, getIdentifierConverter());
    }
}