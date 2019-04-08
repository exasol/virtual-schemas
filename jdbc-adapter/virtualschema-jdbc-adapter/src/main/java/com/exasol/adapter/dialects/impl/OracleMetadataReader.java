package com.exasol.adapter.dialects.impl;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.jdbc.TableMetadataReader;

public class OracleMetadataReader extends BaseRemoteMetadataReader {
    public OracleMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new OracleTableMetadataReader(createColumnMetadataReader(), this.properties);
    }
}