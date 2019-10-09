package com.exasol.adapter.dialects.redshift;

import java.sql.Connection;
import java.sql.Types;
import java.util.logging.Logger;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements a Redshift-specific column metadata reader.
 */
public class RedshiftColumnMetadataReader extends BaseColumnMetadataReader {
    private static final Logger LOGGER = Logger.getLogger(RedshiftColumnMetadataReader.class.getName());

    /**
     * Create a new instance of the {@link RedshiftColumnMetadataReader}.
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
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.NUMERIC:
            return mapJdbcTypeNumericToDecimalWithFallbackToDouble(jdbcTypeDescription);
        case Types.OTHER:
            return mapJdbcTypeOther(jdbcTypeDescription);
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    protected DataType mapJdbcTypeOther(final JdbcTypeDescription jdbcTypeDescription) {
        final String originalDataTypeName = jdbcTypeDescription.getTypeName();
        if ("double".equals(originalDataTypeName)) {
            return DataType.createDouble();
        } else {
            LOGGER.finer(() -> "Mapping JDBC type OTHER [" + jdbcTypeDescription.getTypeName()
                    + "] to maximum size VARCHAR.");
            return DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8);
        }
    }
}