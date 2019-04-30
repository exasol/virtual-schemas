package com.exasol.adapter.dialects.impala;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.*;

/**
 * This class reads Impala-specific database metadata
 */
public class ImpalaMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link ImpalaMetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public ImpalaMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new ImpalaColumnMetadataReader(this.connection, this.properties);
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new ImpalaTableMetadataReader(getColumnMetadataReader(), this.properties);
    }
}
