package com.exasol.adapter.dialects.bigquery;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.ANY_TABLE_TYPE;

import java.sql.Connection;
import java.util.Set;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;

/**
 * Metadata reader that reads BigQuery-specific database metadata.
 */
public class BigQueryMetadataReader extends AbstractRemoteMetadataReader {
    /**
     * Create a new instance of the {@link BigQueryMetadataReader}.
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     */
    public BigQueryMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected ColumnMetadataReader createColumnMetadataReader() {
        return new BigQueryColumnMetadataReader(this.connection, this.properties, this.identifierConverter);
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
        return new BaseIdentifierConverter(IdentifierCaseHandling.INTERPRET_AS_LOWER,
                IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE);
    }
}
