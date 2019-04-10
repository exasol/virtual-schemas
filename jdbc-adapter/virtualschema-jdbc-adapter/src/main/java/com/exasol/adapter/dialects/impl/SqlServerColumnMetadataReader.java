package com.exasol.adapter.dialects.impl;

import java.sql.Connection;
import java.sql.Types;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements a SQLServer-specific column metadata reader
 */
public class SqlServerColumnMetadataReader extends BaseColumnMetadataReader {
    static final int SQLSERVER_BLOB_SIZE = 100;
    static final int SQLSERVER_HIERARCHYID_SIZE = 4000;
    static final int SQLSERVER_MAX_VARCHAR_SIZE = 8000;
    static final int SQLSERVER_MAX_NVARCHAR_SIZE = 4000;
    static final int SQLSERVER_MAX_CLOB_SIZE = 2000000;
    static final int SQLSERVER_TIMESTAMP_TEXT_SIZE = 21;
    static final String SQLSERVER_DATE_TYPE_NAME = "date";
    static final String SQLSERVER_DATETIME2_TYPE_NAME = "datetime2";
    static final String SQLSERVER_GEOMETRY_TYPE_NAME = "geometry";
    static final String SQLSERVER_HIERARCHYID_TYPE_NAME = "hierarchyid";

    /**
     * Create a new instance of a {@link SqlServerColumnMetadataReader}
     *
     * @param connection JDBC connection to the remote data source
     * @param properties user-defined adapter properties
     */
    public SqlServerColumnMetadataReader(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.VARCHAR:
            return mapVarChar(jdbcTypeDescription);
        case Types.TIME:
        case Types.TIME_WITH_TIMEZONE:
            return DataType.createVarChar(SQLSERVER_TIMESTAMP_TEXT_SIZE, DataType.ExaCharset.UTF8);
        case Types.NUMERIC:
            return mapJdbcTypeNumericToDecimalWithFallbackToDouble(jdbcTypeDescription);
        case Types.OTHER:
        case Types.SQLXML:
            return DataType.createVarChar(SqlServerSqlDialect.MAX_SQLSERVER_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
        case Types.CLOB: // xml type in SQL Server
            return DataType.createVarChar(SqlServerSqlDialect.MAX_SQLSERVER_CLOB_SIZE, DataType.ExaCharset.UTF8);
        case Types.BLOB:
            return mapBlob(jdbcTypeDescription);
        case Types.VARBINARY:
        case Types.BINARY:
        case Types.DISTINCT:
            return DataType.createVarChar(SQLSERVER_BLOB_SIZE, DataType.ExaCharset.UTF8);
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    protected DataType mapVarChar(final JdbcTypeDescription jdbcTypeDescription) {
        final String columnTypeName = jdbcTypeDescription.getTypeName();
        if (columnTypeName.equalsIgnoreCase(SQLSERVER_DATE_TYPE_NAME)) {
            return DataType.createDate();
        } else if (columnTypeName.equalsIgnoreCase(SQLSERVER_DATETIME2_TYPE_NAME)) {
            return DataType.createTimestamp(false);
        } else {
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    protected DataType mapBlob(final JdbcTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getTypeName().toLowerCase()) {
        case SQLSERVER_HIERARCHYID_TYPE_NAME:
            return DataType.createVarChar(SQLSERVER_HIERARCHYID_SIZE, DataType.ExaCharset.UTF8);
        case SQLSERVER_GEOMETRY_TYPE_NAME:
            return DataType.createVarChar(SQLSERVER_MAX_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
        default:
            return DataType.createVarChar(SQLSERVER_BLOB_SIZE, DataType.ExaCharset.UTF8);
        }
    }
}