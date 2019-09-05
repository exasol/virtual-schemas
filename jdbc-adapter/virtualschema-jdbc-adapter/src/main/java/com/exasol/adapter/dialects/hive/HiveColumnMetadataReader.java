package com.exasol.adapter.dialects.hive;

import static com.exasol.adapter.dialects.hive.HiveProperties.HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY;

import java.sql.Connection;
import java.sql.Types;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements Hive-specific reading of column metadata.
 */
public class HiveColumnMetadataReader extends BaseColumnMetadataReader {

    /**
     * Create a new instance of the {@link HiveColumnMetadataReader}.
     *
     * @param connection          connection to the remote data source
     * @param properties          user-defined adapter properties
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public HiveColumnMetadataReader(final Connection connection, final AdapterProperties properties,
            final IdentifierConverter identifierConverter) {
        super(connection, properties, identifierConverter);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        if (jdbcTypeDescription.getJdbcType() == Types.DECIMAL) {
            return mapDecimal(jdbcTypeDescription);
        } else {
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    protected DataType mapDecimal(final JdbcTypeDescription jdbcTypeDescription) {
        final int jdbcPrecision = jdbcTypeDescription.getPrecisionOrSize();
        final int scale = jdbcTypeDescription.getDecimalScale();
        if (jdbcPrecision <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
            return DataType.createDecimal(jdbcPrecision, scale);
        } else if (this.properties.containsKey(HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY)) {
            return getNumberTypeFromProperty(HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY);
        } else {
            return DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8);
        }
    }
}
