package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.metadata.DataType;

import java.sql.*;

/**
 * This class implements Teradata-specific reading of column metadata
 */
public class TeradataColumnMetadataReader extends BaseColumnMetadataReader {
    static final int MAX_TERADATA_VARCHAR_SIZE = 32000;

    /**
     * Create a new instance of a {@link TeradataColumnMetadataReader}
     *
     * @param connection JDBC connection through which the column metadata is read from the remote database
     * @param properties user-defined adapter properties
     */
    public TeradataColumnMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.TIME:
        case Types.TIME_WITH_TIMEZONE:
            return DataType.createVarChar(21, DataType.ExaCharset.UTF8);
        case Types.NUMERIC:
            return super.mapJdbcTypeNumericToDecimalWithFallbackToDouble(jdbcTypeDescription);
        case Types.OTHER:
            return getOtherDataType(jdbcTypeDescription);
        case Types.SQLXML:
        case Types.CLOB:
            return DataType.createVarChar(MAX_TERADATA_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
        case Types.BLOB:
        case Types.VARBINARY:
        case Types.BINARY:
        case Types.DISTINCT:
            return DataType.createVarChar(100, DataType.ExaCharset.UTF8);
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    private DataType getOtherDataType(final JdbcTypeDescription jdbcTypeDescription) {
        final String columnTypeName = jdbcTypeDescription.getTypeName();
        if (columnTypeName.startsWith("GEOMETRY")) {
            return DataType.createVarChar(jdbcTypeDescription.getPrecisionOrSize(), DataType.ExaCharset.UTF8);
        } else if (columnTypeName.startsWith("INTERVAL")) {
            return DataType.createVarChar(30, DataType.ExaCharset.UTF8);
        } else if (columnTypeName.startsWith("PERIOD")) {
            return DataType.createVarChar(100, DataType.ExaCharset.UTF8);
        } else {
            return DataType.createVarChar(MAX_TERADATA_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
        }
    }
}
