package com.exasol.adapter.dialects.postgresql;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.*;

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
    public BaseTableMetadataReader createTableMetadataReader() {
        return new PostgreSQLTableMetadataReader(getColumnMetadataReader(), this.properties);
    }

    @Override
    public ColumnMetadataReader createColumnMetadataReader() {
        return new PostgreSQLColumnMetadataReader(this.connection, this.properties);
    }
}