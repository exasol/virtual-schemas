package com.exasol.adapter.dialects.db2;

import java.sql.Connection;
import java.sql.Types;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements DB2-specific reading of column metadata.
 */
public class DB2ColumnMetadataReader extends BaseColumnMetadataReader {
    /**
     * Create a new instance of the {@link DB2ColumnMetadataReader}.
     *
     * @param connection          connection to the remote data source
     * @param properties          user-defined adapter properties
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public DB2ColumnMetadataReader(final Connection connection, final AdapterProperties properties,
            final IdentifierConverter identifierConverter) {
        super(connection, properties, identifierConverter);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        final int size = jdbcTypeDescription.getPrecisionOrSize();
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.CLOB:
        case Types.OTHER:
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
        case Types.BINARY:
            return DataType.createChar(size * 2, DataType.ExaCharset.ASCII);
        case Types.VARBINARY:
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
