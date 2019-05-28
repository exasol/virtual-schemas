package com.exasol.adapter.jdbc;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;

public class DummyRemoteMetadataReader extends AbstractRemoteMetadataReader {
    public DummyRemoteMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected IdentifierConverter createIdentifierConverter() {
        // TODO Auto-generated method stub
        return null;
    }

}
