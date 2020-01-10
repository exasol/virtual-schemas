package com.exasol.adapter.dialects.db2;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.*;

/**
 * This class reads and DB2-specific database metadata.
 */
public class DB2MetadataReader extends AbstractRemoteMetadataReader {
    /**
     * Create a new instance of the {@link DB2MetadataReader}.
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public DB2MetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new DB2ColumnMetadataReader(this.connection, this.properties, getIdentifierConverter());
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new BaseTableMetadataReader(this.connection, this.columnMetadataReader, this.properties,
                this.identifierConverter);
    }

    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return BaseIdentifierConverter.createDefault();
    }
}