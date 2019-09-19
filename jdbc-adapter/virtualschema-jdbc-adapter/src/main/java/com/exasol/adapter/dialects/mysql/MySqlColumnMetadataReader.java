package com.exasol.adapter.dialects.mysql;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.metadata.DataType;

import java.sql.Connection;
import java.sql.Types;

/**
 * This class implements MySQL-specific reading of column metadata.
 */
public class MySqlColumnMetadataReader extends BaseColumnMetadataReader {

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
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }
}
