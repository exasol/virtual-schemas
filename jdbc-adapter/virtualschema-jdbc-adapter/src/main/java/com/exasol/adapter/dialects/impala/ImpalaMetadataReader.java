package com.exasol.adapter.dialects.impala;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.*;

/**
 * This class reads Impala-specific database metadata
 */
public class ImpalaMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of a {@link ImpalaMetadataReader}
     *
     * @param connection database connection through which the reader retrieves the metadata from the remote source
     * @param properties user-defined properties
     */
    public ImpalaMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return new BaseIdentifierConverter(IdentifierCaseHandling.INTERPRET_AS_LOWER,
              IdentifierCaseHandling.INTERPRET_AS_LOWER);
    }
}
