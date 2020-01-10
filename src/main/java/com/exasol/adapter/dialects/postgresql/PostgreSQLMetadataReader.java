package com.exasol.adapter.dialects.postgresql;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.*;

/**
 * This class implements a reader for PostgreSQL-specific metadata.
 */
public class PostgreSQLMetadataReader extends AbstractRemoteMetadataReader {
    /**
     * Create a new instance of the {@link PostgreSQLMetadataReader}.
     *
     * @param connection connection to the PostgreSQL database
     * @param properties user-defined adapter properties
     */
    public PostgreSQLMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    public BaseTableMetadataReader createTableMetadataReader() {
        return new PostgreSQLTableMetadataReader(this.connection, getColumnMetadataReader(), this.properties,
                getIdentifierConverter());
    }

    @Override
    public ColumnMetadataReader createColumnMetadataReader() {
        return new PostgreSQLColumnMetadataReader(this.connection, this.properties, getIdentifierConverter());
    }

    @Override
    public IdentifierConverter createIdentifierConverter() {
        return new PostgreSQLIdentifierConverter(this.properties);
    }
}