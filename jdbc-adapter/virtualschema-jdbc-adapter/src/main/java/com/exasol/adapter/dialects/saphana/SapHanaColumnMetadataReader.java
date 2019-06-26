package com.exasol.adapter.dialects.saphana;

import java.sql.*;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.metadata.*;

/**
 * This class implements SAP HANA-specific reading of column metadata
 */
public class SapHanaColumnMetadataReader extends BaseColumnMetadataReader {

    /**
     * Create a new instance of a {@link SapHanaColumnMetadataReader}
     *
     * @param connection          JDBC connection to the remote data source
     * @param properties          user-defined adapter properties
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public SapHanaColumnMetadataReader(final Connection connection, final AdapterProperties properties,
            final IdentifierConverter identifierConverter) {
        super(connection, properties, identifierConverter);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        if (jdbcTypeDescription.getJdbcType() == Types.DECIMAL) {
            return DataType.createDouble();
        }
        return super.mapJdbcType(jdbcTypeDescription);
    }
}
