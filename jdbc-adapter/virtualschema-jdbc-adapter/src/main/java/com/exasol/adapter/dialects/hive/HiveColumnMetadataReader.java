package com.exasol.adapter.dialects.hive;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.metadata.*;

import java.sql.*;
import java.util.regex.*;

import static com.exasol.adapter.dialects.hive.HiveProperties.HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY;

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
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.DECIMAL:
            return mapDecimal(jdbcTypeDescription);
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    protected DataType mapDecimal(final JdbcTypeDescription jdbcTypeDescription) {
        final int jdbcPrecision = jdbcTypeDescription.getPrecisionOrSize();
        final int scale = jdbcTypeDescription.getDecimalScale();
        if (jdbcPrecision <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
            return DataType.createDecimal(jdbcPrecision, scale);
        } else if (this.properties.containsKey(HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY)) {
            return getHiveNumberTypeFromProperty();
        } else {
            return DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8);
        }
    }

    private DataType getHiveNumberTypeFromProperty() {
        final Pattern pattern = Pattern.compile("\\s*(\\d+)\\s*,\\s*(\\d+)\\s*");
        final String precisionAndScale = this.properties.get(HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY);
        final Matcher matcher = pattern.matcher(precisionAndScale);
        if (matcher.matches()) {
            final int precision = Integer.parseInt(matcher.group(1));
            final int scale = Integer.parseInt(matcher.group(2));
            return DataType.createDecimal(precision, scale);
        } else {
            throw new IllegalArgumentException("Unable to parse adapter property "
                    + HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY + " value \"" + precisionAndScale
                    + " into a number precision and scale. The required format is \"<precision>.<scale>\", where both are integer numbers.");
        }
    }
}
