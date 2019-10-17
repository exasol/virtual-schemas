package com.exasol.adapter.dialects.exasol;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements Exasol-specific reading of column metadata.
 */
public class ExasolColumnMetadataReader extends BaseColumnMetadataReader {
    static final int EXASOL_INTERVAL_DAY_TO_SECONDS = -104;
    static final int EXASOL_INTERVAL_YEAR_TO_MONTHS = -103;
    static final int EXASOL_GEOMETRY = 123;
    static final int EXASOL_TIMESTAMP = 124;
    static final int EXASOL_HASHTYPE = 125;
    private static final int DEFAULT_HASHTYPE_BYTESIZE = 16;
    private static final int DEFAULT_INTERVAL_DAY_TO_SECOND_FRACTION = 3;
    private static final int DEFAULT_INTERVAL_DAY_TO_SECOND_PRECISION = 2;
    private static final int DEFAULT_INTERVAL_YEAR_TO_MONTH_PRECISION = 2;
    private static final int DEFAULT_SPACIAL_REFERENCE_SYSTEM_IDENTIFIER = 3857;

    /**
     * Create a new instance of the {@link ExasolColumnMetadataReader}.
     *
     * @param connection          JDBC connection through which the column metadata is read from the remote database
     * @param properties          user-defined adapter properties
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public ExasolColumnMetadataReader(final Connection connection, final AdapterProperties properties,
            final IdentifierConverter identifierConverter) {
        super(connection, properties, identifierConverter);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getJdbcType()) {
        case EXASOL_INTERVAL_DAY_TO_SECONDS:
            return DataType.createIntervalDaySecond(DEFAULT_INTERVAL_DAY_TO_SECOND_PRECISION,
                    DEFAULT_INTERVAL_DAY_TO_SECOND_FRACTION);
        case EXASOL_INTERVAL_YEAR_TO_MONTHS:
            return DataType.createIntervalYearMonth(DEFAULT_INTERVAL_YEAR_TO_MONTH_PRECISION);
        case EXASOL_GEOMETRY:
            return DataType.createGeometry(DEFAULT_SPACIAL_REFERENCE_SYSTEM_IDENTIFIER);
        case EXASOL_TIMESTAMP:
            return DataType.createTimestamp(true);
        case EXASOL_HASHTYPE:
            return DataType.createHashtype(DEFAULT_HASHTYPE_BYTESIZE);
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }
}
