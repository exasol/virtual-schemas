package com.exasol.adapter.dialects.hive;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.BaseRemoteMetadataReader;
import com.exasol.adapter.jdbc.IdentifierCaseHandling;

/**
 * This class reads Hive-specific database metadata
 */
public class HiveMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link HiveMetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public HiveMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return new IdentifierConverter(IdentifierCaseHandling.INTERPRET_AS_LOWER,
                IdentifierCaseHandling.INTERPRET_AS_LOWER);
    }
}