package com.exasol.adapter.dialects.bigquery;

import java.sql.*;
import java.util.*;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.ANY_TABLE_TYPE;

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
