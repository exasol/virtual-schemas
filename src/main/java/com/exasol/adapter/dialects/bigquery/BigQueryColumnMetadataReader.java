package com.exasol.adapter.dialects.bigquery;

import java.sql.Connection;
import java.sql.Types;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements BigQuery-specific reading of column metadata.
 */
public class BigQueryColumnMetadataReader extends BaseColumnMetadataReader {
    /**
     * Create a new instance of the {@link BigQueryColumnMetadataReader}.
     *
     * @param connection          connection to the remote data source
     * @param properties          user-defined adapter properties
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public BigQueryColumnMetadataReader(final Connection connection, final AdapterProperties properties,
            final IdentifierConverter identifierConverter) {
        super(connection, properties, identifierConverter);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        if (jdbcTypeDescription.getJdbcType() == Types.TIME) {
            return DataType.createVarChar(30, DataType.ExaCharset.UTF8);
        }
        return super.mapJdbcType(jdbcTypeDescription);
    }
}
