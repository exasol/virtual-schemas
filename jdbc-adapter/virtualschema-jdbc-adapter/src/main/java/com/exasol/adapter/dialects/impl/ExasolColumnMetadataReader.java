package com.exasol.adapter.dialects.impl;

import java.sql.Connection;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements Exasol-specific reading of column metadata
 */
public class ExasolColumnMetadataReader extends BaseColumnMetadataReader {
    static final int EXASOL_INTERVAL_DAY_TO_SECONDS = -104;
    static final int EXASOL_INTERVAL_YEAR_TO_MONTHS = -103;
    static final int EXASOL_GEOMETRY = 123;
    static final int EXASOL_TIMESTAMP = 124;

    /**
     * Create a new instance of a {@link ExasolColumnMetadataReader}
     *
     * @param connection JDBC connection through which the column metadata is read from the remote database
     * @param properties user-defined adapter properties
     */
    public ExasolColumnMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getJdbcType()) {
        case EXASOL_INTERVAL_DAY_TO_SECONDS:
            return DataType.createIntervalDaySecond(2, 3);
        case EXASOL_INTERVAL_YEAR_TO_MONTHS:
            return DataType.createIntervalYearMonth(2);
        case EXASOL_GEOMETRY:
            return DataType.createGeometry(3857);
        case EXASOL_TIMESTAMP:
            return DataType.createTimestamp(true);
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }
}
