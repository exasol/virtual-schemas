package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.metadata.DataType;

import java.sql.*;

public class DB2ColumnMetadataReader extends BaseColumnMetadataReader {
    static final int DB2SQL_VARCHAR_WITH_EXASOL_MAX_SIZE = 1111;
    static final int DB2SQL_CHAR = -2;
    static final int DB2SQL_VARCHAR = -3;

    public DB2ColumnMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        final int size = jdbcTypeDescription.getPrecisionOrSize();
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.CLOB:
        case DB2SQL_VARCHAR_WITH_EXASOL_MAX_SIZE:
            return DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
        case Types.TIMESTAMP:
            return DataType.createVarChar(32, DataType.ExaCharset.UTF8);
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.LONGVARCHAR:
        case Types.CHAR:
        case Types.NCHAR:
        case Types.LONGNVARCHAR:
            return getLiteralDataType(size);
        case DB2SQL_CHAR:
            return DataType.createChar(size * 2, DataType.ExaCharset.ASCII);
        case DB2SQL_VARCHAR:
            return DataType.createVarChar(size * 2, DataType.ExaCharset.ASCII);
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    private DataType getLiteralDataType(final int size) {
        final DataType.ExaCharset charset = DataType.ExaCharset.UTF8;
        if (size <= DataType.MAX_EXASOL_VARCHAR_SIZE) {
            return DataType.createVarChar(size, charset);
        } else {
            return DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, charset);
        }
    }
}
