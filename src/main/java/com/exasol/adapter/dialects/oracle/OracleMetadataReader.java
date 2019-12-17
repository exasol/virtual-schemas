package com.exasol.adapter.dialects.oracle;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.*;

/**
 * This class reads Oracle-specific database metadata.
 */
public class OracleMetadataReader extends AbstractRemoteMetadataReader {
    /**
     * Create a new instance of the {@link OracleMetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public OracleMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new OracleTableMetadataReader(this.connection, getColumnMetadataReader(), this.properties,
                super.getIdentifierConverter());
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new OracleColumnMetadataReader(this.connection, this.properties, getIdentifierConverter());
    }

    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return BaseIdentifierConverter.createDefault();
    }
}