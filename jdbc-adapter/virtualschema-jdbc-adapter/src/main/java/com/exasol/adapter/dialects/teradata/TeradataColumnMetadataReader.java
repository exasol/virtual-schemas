package com.exasol.adapter.dialects.teradata;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.ANY_COLUMN;

import java.sql.*;
import java.util.Collections;
import java.util.List;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.jdbc.BaseColumnMetadataReader;
import com.exasol.adapter.jdbc.RemoteMetadataReaderException;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements Teradata-specific reading of column metadata
 */
public class TeradataColumnMetadataReader extends BaseColumnMetadataReader {
    static final int MAX_TERADATA_VARCHAR_SIZE = 32000;

    /**
     * Create a new instance of a {@link TeradataColumnMetadataReader}
     *
     * @param connection          JDBC connection through which the column metadata is read from the remote database
     * @param properties          user-defined adapter properties
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public TeradataColumnMetadataReader(final Connection connection, final AdapterProperties properties,
            final IdentifierConverter identifierConverter) {
        super(connection, properties, identifierConverter);
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

    @Override
    protected List<ColumnMetadata> mapColumns(final String catalogName, final String schemaName,
            final String tableName) {
        try (final ResultSet remoteColumns = this.connection.getMetaData().getColumns(catalogName, schemaName,
                tableName, ANY_COLUMN)) {
            return getColumnsFromResultSet(remoteColumns);
        } catch (final SQLException exception) {
            if (isError3087(exception)) {
                LOGGER.finer("Caught Teradata error 3087 when trying to read column data."
                        + " This happens when the view the columns belong to is invalid. Ignoring columns.");
                return Collections.emptyList();
            } else {
                throw new RemoteMetadataReaderException("Unable to read column metadata from remote for catalog \""
                        + catalogName + "\" and schema \"" + schemaName + "\"", exception);
            }
        }
    }

    protected boolean isError3087(final SQLException exception) {
        return exception.getMessage().contains("Teradata Database") && exception.getMessage().contains("Error 3807");
    }

}
