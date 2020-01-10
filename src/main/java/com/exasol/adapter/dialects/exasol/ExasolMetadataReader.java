package com.exasol.adapter.dialects.exasol;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.*;

/**
 * This class reads Exasol-specific database metadata.
 */
public class ExasolMetadataReader extends AbstractRemoteMetadataReader {
    /**
     * Create a new instance of the {@link ExasolMetadataReader}.
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public ExasolMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new ExasolColumnMetadataReader(this.connection, this.properties, getIdentifierConverter());
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
