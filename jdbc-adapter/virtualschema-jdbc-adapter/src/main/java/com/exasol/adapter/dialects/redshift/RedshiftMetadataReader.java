package com.exasol.adapter.dialects.redshift;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.*;

/**
 * This class implements a Redshift-specific metadata reader
 */
public class RedshiftMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link RedshiftMetadataReader}
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     */
    public RedshiftMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new RedshiftColumnMetadataReader(this.connection, this.properties);
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new RedshiftTableMetadataReader(columnMetadataReader, properties);
    }
}