package com.exasol.adapter.dialects.mysql;

import java.sql.Connection;
import java.sql.Types;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements MySQL-specific reading of column metadata.
 */
public class MySqlColumnMetadataReader extends BaseColumnMetadataReader {
    private static final String TEXT_DATA_TYPE_NAME = "TEXT";
    protected static final int TEXT_DATA_TYPE_SIZE = 65535;

    /**
     * Create a new instance of the {@link MySqlColumnMetadataReader}.
     *
     * @param connection          JDBC connection through which the column metadata is read from the remote database
     * @param properties          user-defined adapter properties
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public MySqlColumnMetadataReader(final Connection connection, final AdapterProperties properties,
            final IdentifierConverter identifierConverter) {
        super(connection, properties, identifierConverter);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.TIME:
            return DataType.createTimestamp(false);
        case Types.BINARY:
            return DataType.createUnsupported();
        case Types.LONGVARCHAR:
            return convertVarChar(jdbcTypeDescription);
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    private DataType convertVarChar(final JdbcTypeDescription jdbcTypeDescription) {
        final int size = getVarcharSize(jdbcTypeDescription);
        final int octetLength = jdbcTypeDescription.getByteSize();
        final DataType.ExaCharset charset = (octetLength == size) ? DataType.ExaCharset.ASCII
                : DataType.ExaCharset.UTF8;
        if (size <= DataType.MAX_EXASOL_VARCHAR_SIZE) {
            final int precision = getVarcharPrecision(size);
            return DataType.createVarChar(precision, charset);
        } else {
            return DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, charset);
        }
    }

    private int getVarcharPrecision(int size) {
        if (size == 0) {
            return DataType.MAX_EXASOL_VARCHAR_SIZE;
        } else {
            return size;
        }
    }

    private int getVarcharSize(final JdbcTypeDescription jdbcTypeDescription) {
        final String typeName = jdbcTypeDescription.getTypeName();
        if (typeName.equals(TEXT_DATA_TYPE_NAME)) {
            return TEXT_DATA_TYPE_SIZE;
        } else {
            return jdbcTypeDescription.getPrecisionOrSize();
        }
    }
}
