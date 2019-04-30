package com.exasol.adapter.dialects.oracle;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.*;

/**
 * This class reads and Oracle-specific database metadata
 */
public class OracleMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link OracleMetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public OracleMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new OracleTableMetadataReader(this.connection, super.getColumnMetadataReader(), this.properties,
                super.getIdentifierConverter());
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new OracleColumnMetadataReader(this.connection, this.properties, getIdentifierConverter());
    }
}