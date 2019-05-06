package com.exasol.adapter.dialects.athena;

import java.sql.Connection;
import java.util.Set;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.*;

/**
 * Metadata reader that reads AWS-Athena-specific database metadata
 */
public class AthenaMetadataReader extends BaseRemoteMetadataReader {
    /**
     * Create a new instance of an {@link AthenaMetadataReader}
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     */
    public AthenaMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    public Set<String> getSupportedTableTypes() {
        return RemoteMetadataReaderConstants.ANY_TABLE_TYPE;
    }

    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return new BaseIdentifierConverter(IdentifierCaseHandling.INTERPRET_AS_UPPER,
                IdentifierCaseHandling.INTERPRET_AS_UPPER);
    }
}