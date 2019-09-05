package com.exasol.adapter.dialects.saphana;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.ANY_TABLE_TYPE;

import java.sql.Connection;
import java.util.Set;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.*;

/**
 * Metadata reader that reads SAP HANA-specific database metadata.
 */
public class SapHanaMetadataReader extends AbstractRemoteMetadataReader {
    /**
     * Create a new instance of a {@link SapHanaMetadataReader}.
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     */
    public SapHanaMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new BaseColumnMetadataReader(this.connection, this.properties, this.identifierConverter);
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new BaseTableMetadataReader(this.connection, this.columnMetadataReader, this.properties,
                this.identifierConverter);
    }

    @Override
    public Set<String> getSupportedTableTypes() {
        return ANY_TABLE_TYPE;
    }

    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return new BaseIdentifierConverter(IdentifierCaseHandling.INTERPRET_AS_UPPER,
                IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE);
    }
}
