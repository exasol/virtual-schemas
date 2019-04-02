package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;

public class ColumnMetadataReader {
    public static final Logger LOGGER = Logger.getLogger(ColumnMetadataReader.class.getName());
    public static final String NAME_COLUMN = "COLUMN_NAME";
    public static final String DATA_TYPE_COLUMN = "DATA_TYPE";
    public static final String SIZE_COLUMN = "COLUMN_SIZE";
    public static final String PRECISION_COLUMN = "NUM_PREC_RADIX";
    public static final String SCALE_COLUMN = "DECIMAL_DIGITS";
    public static final String CHAR_OCTET_LENGTH_COLUMN = "CHAR_OCTET_LENGTH";
    public static final String TYPE_NAME_COLUMN = "TYPE_NAME";
    public static final String REMARKS_COLUMN = "REMARKS";
    public static final String DEFAULT_VALUE_COLUMN = "COLUMN_DEF";
    public static final String AUTOINCREMENT_COLUMN = "IS_AUTOINCREMENT";
    public static final String NULLABLE_COLUMN = "IS_NULLABLE";
    private static final boolean DEFAULT_NULLABLE = true;
    private final Connection connection;

    public ColumnMetadataReader(final Connection connection, final SqlDialect sqlDialect) {
        this.connection = connection;
    }

    public List<ColumnMetadata> mapColumns(final String tableName) {
        final List<ColumnMetadata> columns = new ArrayList<>();
        try (final ResultSet remoteColumns = this.connection.getMetaData().getColumns(ANY_CATALOG, ANY_SCHEMA,
                tableName, ANY_COLUMN)) {
            while (remoteColumns.next()) {
                final ColumnMetadata metadata = mapColumn(remoteColumns);
                columns.add(metadata);
            }
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to read column metadata from remote", exception);
        }
        return columns;
    }

    private ColumnMetadata mapColumn(final ResultSet remoteColumn) throws SQLException {
        final String columnName = readColumnName(remoteColumn);
        final int jdbcType = remoteColumn.getInt(DATA_TYPE_COLUMN);
        final int decimalScale = remoteColumn.getInt(SCALE_COLUMN);
        final int precisionOrSize = remoteColumn.getInt(SIZE_COLUMN);
        final int charOctedLength = remoteColumn.getInt(CHAR_OCTET_LENGTH_COLUMN);
        final String typeName = remoteColumn.getString(TYPE_NAME_COLUMN);
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(jdbcType, decimalScale, precisionOrSize,
                charOctedLength, typeName);
        final DataType colType = mapJdbcType(jdbcTypeDescription);
        final boolean isNullable = isRemoteColumnNullable(remoteColumn, columnName);
        final boolean isIdentity = isAutoIncrementColmun(remoteColumn, columnName);
        final String defaultValue = readDefaultValue(remoteColumn);
        final String comment = readComment(remoteColumn);
        final String columnTypeName = readColumnTypeName(remoteColumn);
        final String adapterNotes = ColumnAdapterNotes.serialize(new ColumnAdapterNotes(jdbcType, columnTypeName));
        return ColumnMetadata.builder() //
                .name(columnName) //
                .adapterNotes(adapterNotes) //
                .type(colType) //
                .nullable(isNullable) //
                .identity(isIdentity) //
                .defaultValue(defaultValue) //
                .comment(comment).build();
    }

    private boolean isRemoteColumnNullable(final ResultSet remoteColumn, final String columnName) {
        try {
            final String nullable = remoteColumn.getString(NULLABLE_COLUMN);
            return !JDBC_FALSE.equalsIgnoreCase(nullable);
        } catch (final SQLException exception) {
            LOGGER.warning(() -> "Caught an SQL exception trying to determine whether column \"" + columnName
                    + "\" is nullable: " + exception.getMessage());
            LOGGER.warning(() -> "Assuming column \"" + columnName + "\" to be nullable.");
            return DEFAULT_NULLABLE;
        }
    }

    private boolean isAutoIncrementColmun(final ResultSet remoteColumn, final String columnName) {
        try {
            final String identity = remoteColumn.getString(AUTOINCREMENT_COLUMN);
            return JDBC_TRUE.equalsIgnoreCase(identity);
        } catch (final SQLException exception) {
            LOGGER.warning(() -> "Caught an SQL exception trying to determine whether column \"" + columnName
                    + "\" is an auto-increment column: " + exception.getMessage());
            LOGGER.warning(() -> "Assuming that column \"" + columnName + "\" is not incremented automatically.");
            return false;
        }
    }

    private String readDefaultValue(final ResultSet remoteColumn) {
        try {
            if (remoteColumn.getString(DEFAULT_VALUE_COLUMN) != null) {
                return remoteColumn.getString(DEFAULT_VALUE_COLUMN);
            } else {
                return "";
            }
        } catch (final SQLException ex) {
            return "";
        }
    }

    private String readComment(final ResultSet remoteColumn) {
        try {
            final String comment = remoteColumn.getString(REMARKS_COLUMN);
            if ((comment != null) && !comment.isEmpty()) {
                return comment;
            } else {
                return "";
            }
        } catch (final SQLException ex) {
            // ignore me
            return "";
        }
    }

    private String readColumnTypeName(final ResultSet remoteColumn) throws SQLException {
        String columnTypeName = remoteColumn.getString(TYPE_NAME_COLUMN);
        if (columnTypeName == null) {
            columnTypeName = "";
        }
        return columnTypeName;
    }

    protected String readColumnName(final ResultSet columns) throws SQLException {
        return columns.getString(NAME_COLUMN);
    }

    public final DataType mapJdbcType(final JdbcTypeDescription jdbcType) throws SQLException {
        DataType type = dialectSpecificMapJdbcType(jdbcType);
        if (type == null) {
            type = mapJdbcToExasolDataType(jdbcType);
        }
        return type;
    }

    @Deprecated
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcType) throws SQLException {
        return null; // FIXME: replace
    }

    private static DataType mapJdbcToExasolDataType(final JdbcTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.TINYINT:
        case Types.SMALLINT:
            return convertSmallInteger(jdbcTypeDescription);
        case Types.INTEGER:
            return convertInteger(jdbcTypeDescription);
        case Types.BIGINT:
            return convertBigInteger(jdbcTypeDescription);
        case Types.DECIMAL:
            return convertDecimal(jdbcTypeDescription);
        case Types.NUMERIC:
            return fallBackToMaximumSizeVarChar();
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
            return DataType.createDouble();
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR:
            return convertVarChar(jdbcTypeDescription);
        case Types.CHAR:
        case Types.NCHAR:
            return convertChar(jdbcTypeDescription);
        case Types.DATE:
            return DataType.createDate();
        case Types.TIMESTAMP:
            return DataType.createTimestamp(false);
        case Types.TIME:
            return fallBackToMaximumSizeVarChar();
        case Types.BIT:
        case Types.BOOLEAN:
            return DataType.createBool();
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BLOB:
        case Types.CLOB:
        case Types.NCLOB:
            return fallBackToMaximumSizeVarChar();
        case Types.OTHER:
        case Types.JAVA_OBJECT:
        case Types.DISTINCT:
        case Types.STRUCT:
        case Types.ARRAY:
        case Types.REF:
        case Types.DATALINK:
        case Types.SQLXML:
        case Types.NULL:
        default:
            throw new RemoteMetadataReaderException("Unsupported JBDC data type \"" + jdbcTypeDescription.getJdbcType()
                    + "\" found trying to map remote schema metadata to Exasol.");
        }
    }

    private static DataType convertSmallInteger(final JdbcTypeDescription jdbcTypeDescription) {
        final DataType colType;
        final int precision = jdbcTypeDescription.getPrecisionOrSize() == 0 ? 9
                : jdbcTypeDescription.getPrecisionOrSize();
        colType = DataType.createDecimal(precision, 0);
        return colType;
    }

    private static DataType convertInteger(final JdbcTypeDescription jdbcTypeDescription) {
        final DataType colType;
        if (jdbcTypeDescription.getPrecisionOrSize() <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
            final int precision = jdbcTypeDescription.getPrecisionOrSize() == 0 ? 18
                    : jdbcTypeDescription.getPrecisionOrSize();
            colType = DataType.createDecimal(precision, 0);
        } else {
            colType = fallBackToMaximumSizeVarChar();
        }
        return colType;
    }

    private static DataType convertBigInteger(final JdbcTypeDescription jdbcTypeDescription) {
        final DataType colType;
        if (jdbcTypeDescription.getPrecisionOrSize() <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
            final int precision = jdbcTypeDescription.getPrecisionOrSize() == 0 ? 36
                    : jdbcTypeDescription.getPrecisionOrSize();
            colType = DataType.createDecimal(precision, 0);
        } else {
            colType = fallBackToMaximumSizeVarChar();
        }
        return colType;
    }

    private static DataType convertDecimal(final JdbcTypeDescription jdbcTypeDescription) {
        final DataType colType;
        if (jdbcTypeDescription.getPrecisionOrSize() <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
            colType = DataType.createDecimal(jdbcTypeDescription.getPrecisionOrSize(),
                    jdbcTypeDescription.getDecimalScale());
        } else {
            colType = fallBackToMaximumSizeVarChar();
        }
        return colType;
    }

    private static DataType fallBackToMaximumSizeVarChar() {
        return DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
    }

    private static DataType convertVarChar(final JdbcTypeDescription jdbcTypeDescription) {
        final DataType colType;
        final DataType.ExaCharset charset = (jdbcTypeDescription.getCharOctedLength() == jdbcTypeDescription
                .getPrecisionOrSize()) ? DataType.ExaCharset.ASCII : DataType.ExaCharset.UTF8;
        if (jdbcTypeDescription.getPrecisionOrSize() <= DataType.MAX_EXASOL_VARCHAR_SIZE) {
            final int precision = jdbcTypeDescription.getPrecisionOrSize() == 0 ? DataType.MAX_EXASOL_VARCHAR_SIZE
                    : jdbcTypeDescription.getPrecisionOrSize();
            colType = DataType.createVarChar(precision, charset);
        } else {
            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, charset);
        }
        return colType;
    }

    private static DataType convertChar(final JdbcTypeDescription jdbcTypeDescription) {
        final DataType colType;
        final DataType.ExaCharset charset = (jdbcTypeDescription.getCharOctedLength() == jdbcTypeDescription
                .getPrecisionOrSize()) ? DataType.ExaCharset.ASCII : DataType.ExaCharset.UTF8;
        if (jdbcTypeDescription.getPrecisionOrSize() <= DataType.MAX_EXASOL_CHAR_SIZE) {
            colType = DataType.createChar(jdbcTypeDescription.getPrecisionOrSize(), charset);
        } else {
            if (jdbcTypeDescription.getPrecisionOrSize() <= DataType.MAX_EXASOL_VARCHAR_SIZE) {
                colType = DataType.createVarChar(jdbcTypeDescription.getPrecisionOrSize(), charset);
            } else {
                colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, charset);
            }
        }
        return colType;
    }
}