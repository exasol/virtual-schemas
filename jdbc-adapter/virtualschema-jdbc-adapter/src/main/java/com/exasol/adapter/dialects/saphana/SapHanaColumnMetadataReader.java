package com.exasol.adapter.dialects.saphana;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.metadata.*;

import java.sql.*;
import java.util.logging.*;

/**
 * This class implements SAP HANA-specific reading of column metadata
 */
public class SapHanaColumnMetadataReader extends BaseColumnMetadataReader {
    private static final Logger LOGGER = Logger.getLogger(ResultSetMetadataReader.class.getName());

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
        LOGGER.fine("Type name: " + jdbcTypeDescription.getTypeName());
        LOGGER.fine("JDBC type: " + jdbcTypeDescription.getJdbcType());
        LOGGER.fine("Precision: " + jdbcTypeDescription.getPrecisionOrSize());
        LOGGER.fine("Scale: " + jdbcTypeDescription.getDecimalScale());
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.DECIMAL:
            return mapDecimal(jdbcTypeDescription.getPrecisionOrSize(), jdbcTypeDescription.getDecimalScale());
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    private DataType mapDecimal(final int precision, final int scale) {
        if (scale > 0) {
            return convertDecimal(precision, scale);
        } else {
            return DataType.createDouble();
        }
    }
}
