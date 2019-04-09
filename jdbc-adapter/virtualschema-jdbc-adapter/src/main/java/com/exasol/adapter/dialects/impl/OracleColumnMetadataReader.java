package com.exasol.adapter.dialects.impl;

import java.sql.Connection;
import java.sql.Types;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements Oracle-specific reading of column metadata
 */
public class OracleColumnMetadataReader extends BaseColumnMetadataReader {
    private static final int ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE = -101;
    private static final int ORACLE_TIMESTAMP_WITH_TIME_ZONE = -102;
    private static final int ORACLE_BINARY_FLOAT = 100;
    private static final int ORACLE_BINARY_DOUBLE = 101;
    private static final int ORACLE_CLOB = Types.OTHER;
    private static final int INTERVAL_DAY_TO_SECOND = -104;
    private static final int INTERVAL_YEAR_TO_MONTH = -103;
    static final String ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY = "ORACLE_CAST_NUMBER_TO_DECIMAL";
    static final int ORACLE_MAGIC_NUMBER_SCALE = -127;

    /**
     * Create a new instance of an {@link OracleColumnMetadataReader}
     *
     * @param connection connection to the remote data source
     */
    public OracleColumnMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            return mapNumericType(jdbcTypeDescription);
        case ORACLE_CLOB:
        case INTERVAL_YEAR_TO_MONTH:
        case INTERVAL_DAY_TO_SECOND:
        case ORACLE_TIMESTAMP_WITH_TIME_ZONE:
        case ORACLE_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        case ORACLE_BINARY_FLOAT:
        case ORACLE_BINARY_DOUBLE:
            return DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    protected DataType mapNumericType(final JdbcTypeDescription jdbcTypeDescription) {
        final int decimalPrec = jdbcTypeDescription.getPrecisionOrSize();
        final int decimalScale = jdbcTypeDescription.getDecimalScale();
        if (decimalScale == ORACLE_MAGIC_NUMBER_SCALE) {
            return workAroundNumberWithoutScaleAndPrecision();
        }
        if (decimalPrec <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
            return DataType.createDecimal(decimalPrec, decimalScale);
        } else {
            return workAroundNumberWithoutScaleAndPrecision();
        }
    }

    // Oracle JDBC driver returns scale -127 if NUMBER data type was specified
    // without scale and precision. Convert to VARCHAR.
    // See http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#i16209
    // and https://docs.oracle.com/cd/E19501-01/819-3659/gcmaz/
    private DataType workAroundNumberWithoutScaleAndPrecision() {
        return getOracleNumberTargetType();
    }

    private DataType getOracleNumberTargetType() {
        if (this.properties.containsKey(ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY)) {
            return getOracleNumberTypeFromProperty();
        } else {
            return DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
        }
    }

    private DataType getOracleNumberTypeFromProperty() {
        final Pattern pattern = Pattern.compile("\\s*(\\d+)\\s*,\\s*(\\d+)\\s*");
        final String oraclePrecisionAndScale = this.properties.get(ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY);
        final Matcher matcher = pattern.matcher(oraclePrecisionAndScale);
        if (matcher.matches()) {
            final int precision = Integer.parseInt(matcher.group(1));
            final int scale = Integer.parseInt(matcher.group(2));
            return DataType.createDecimal(precision, scale);
        } else {
            throw new IllegalArgumentException("Unable to parse adapter property "
                    + ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY + " value \"" + oraclePrecisionAndScale
                    + " into a number precison and scale. The required format is \"<precsion>.<scale>\", where both are integer numbers.");
        }
    }
}