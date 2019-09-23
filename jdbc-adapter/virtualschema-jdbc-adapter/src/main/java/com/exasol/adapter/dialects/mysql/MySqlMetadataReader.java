package com.exasol.adapter.dialects.mysql;

import java.sql.Connection;
import java.util.Set;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.BaseIdentifierConverter;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.*;

/**
 * Metadata reader that reads MySQL-specific database metadata.
 */
public class MySqlMetadataReader extends AbstractRemoteMetadataReader {
    /**
     * Create a new instance of the {@link MySqlMetadataReader}.
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     */
    public MySqlMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new MySqlColumnMetadataReader(this.connection, this.properties, this.identifierConverter);
    }

    @Override
    protected TableMetadataReader createTableMetadataReader() {
        return new BaseTableMetadataReader(this.connection, this.columnMetadataReader, this.properties,
                this.identifierConverter);
    }

    @Override
    protected IdentifierConverter createIdentifierConverter() {
        return new BaseIdentifierConverter(IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE,
                IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE);
    }

    @Override
    public Set<String> getSupportedTableTypes() {
        return RemoteMetadataReaderConstants.ANY_TABLE_TYPE;
    }
}
