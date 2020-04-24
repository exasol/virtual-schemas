package com.exasol.adapter.dialects.postgresql;

import static com.exasol.adapter.dialects.postgresql.PostgreSQLSqlDialect.POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY;

import java.sql.*;
import java.util.logging.Logger;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.jdbc.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements PostgreSQL-specific reading of column metadata.
 */
public class PostgreSQLColumnMetadataReader extends BaseColumnMetadataReader {
    private static final Logger LOGGER = Logger.getLogger(PostgreSQLColumnMetadataReader.class.getName());
    private static final String POSTGRES_VARBIT_TYPE_NAME = "varbit";

    /**
     * Create a new instance of the {@link PostgreSQLColumnMetadataReader}.
     *
     * @param connection          JDBC connection to the remote data source
     * @param properties          user-defined adapter properties
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public PostgreSQLColumnMetadataReader(final Connection connection, final AdapterProperties properties,
            final IdentifierConverter identifierConverter) {
        super(connection, properties, identifierConverter);
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.OTHER:
            return mapJdbcTypeOther(jdbcTypeDescription);
        case Types.SQLXML:
        case Types.DISTINCT:
        case Types.BINARY:
            return DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8);
        default:
            return super.mapJdbcType(jdbcTypeDescription);
        }
    }

    protected DataType mapJdbcTypeOther(final JdbcTypeDescription jdbcTypeDescription) {
        if (isVarBitColumn(jdbcTypeDescription)) {
            final int n = jdbcTypeDescription.getPrecisionOrSize();
            LOGGER.finer(() -> "Mapping PostgreSQL datatype \"OTHER:varbit\" to VARCHAR(" + n + ")");
            return DataType.createVarChar(n, DataType.ExaCharset.UTF8);
        } else {
            LOGGER.finer(() -> "Mapping PostgreSQL datatype \"" + jdbcTypeDescription.getTypeName()
                    + "\" to maximum VARCHAR()");
            return DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8);
        }
    }

    protected boolean isVarBitColumn(final JdbcTypeDescription jdbcTypeDescription) {
        return jdbcTypeDescription.getTypeName().equals(POSTGRES_VARBIT_TYPE_NAME);
    }

    @Override
    public String readColumnName(final ResultSet columns) throws SQLException {
        if (getIdentifierMapping().equals(PostgreSQLIdentifierMapping.CONVERT_TO_UPPER)) {
            return super.readColumnName(columns).toUpperCase();
        } else {
            return super.readColumnName(columns);
        }
    }

    PostgreSQLIdentifierMapping getIdentifierMapping() {
        return this.properties.containsKey(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY) //
                ? PostgreSQLIdentifierMapping.valueOf(this.properties.get(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY))
                : PostgreSQLIdentifierMapping.CONVERT_TO_UPPER;
    }
}