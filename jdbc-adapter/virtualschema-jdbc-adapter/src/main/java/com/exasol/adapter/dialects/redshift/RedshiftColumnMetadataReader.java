package com.exasol.adapter.dialects.redshift;

import java.sql.Connection;
import java.sql.Types;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements a Redshift-specific column metadata reader
 */
public class RedshiftColumnMetadataReader extends BaseColumnMetadataReader {
    /**
     * Create a new instance of a {@link RedshiftColumnMetadataReader}
     *
     * @param connection          JDBC connection to the remote data source
     * @param properties          user-defined adapter properties
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public RedshiftColumnMetadataReader(final Connection connection, final AdapterProperties properties,
            final IdentifierConverter identifierConverter) {
        super(connection, properties, identifierConverter);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        if (jdbcTypeDescription.getJdbcType() == Types.NUMERIC) {
            return mapJdbcTypeNumericToDecimalWithFallbackToDouble(jdbcTypeDescription);
        } else {
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }
}