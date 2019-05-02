package com.exasol.adapter.dialects.generic;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;

/**
 * This class reads Generic database metadata
 */
public class GenericMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link GenericMetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public GenericMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return new GenericIdentifierConverter(getSchemaAdapterNotes());
    }
}