package com.exasol.adapter.dialects.oracle;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.*;

/**
 * This class reads and Oracle-specific database metadata
 */
public class OracleMetadataReader extends BaseRemoteMetadataReader {
    public OracleMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new OracleTableMetadataReader(getColumnMetadataReader(), this.properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new OracleColumnMetadataReader(this.connection, this.properties);
    }
}