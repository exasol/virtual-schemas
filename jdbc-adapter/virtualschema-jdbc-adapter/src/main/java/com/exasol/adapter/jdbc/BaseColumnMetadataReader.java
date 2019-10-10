package com.exasol.adapter.jdbc;

import static com.exasol.adapter.jdbc.RemoteMetadataReaderConstants.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.adapternotes.ColumnAdapterNotes;
import com.exasol.adapter.dialects.IdentifierConverter;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;

/**
 * This class implements a mapper that reads column metadata from the remote database and converts it into JDBC
 * information.
 */
public class BaseColumnMetadataReader extends AbstractMetadataReader implements ColumnMetadataReader {
    public static final Logger LOGGER = Logger.getLogger(BaseColumnMetadataReader.class.getName());
    public static final String NAME_COLUMN = "COLUMN_NAME";
    public static final String DATA_TYPE_COLUMN = "DATA_TYPE";
    public static final String SIZE_COLUMN = "COLUMN_SIZE";
    public static final String SCALE_COLUMN = "DECIMAL_DIGITS";
    public static final String CHAR_OCTET_LENGTH_COLUMN = "CHAR_OCTET_LENGTH";
    public static final String TYPE_NAME_COLUMN = "TYPE_NAME";
    public static final String REMARKS_COLUMN = "REMARKS";
    public static final String DEFAULT_VALUE_COLUMN = "COLUMN_DEF";
    public static final String AUTOINCREMENT_COLUMN = "IS_AUTOINCREMENT";
    public static final String NULLABLE_COLUMN = "IS_NULLABLE";
    private static final boolean DEFAULT_NULLABLE = true;
    private final IdentifierConverter identifierConverter;

    /**
     * Create a new instance of a {@link ColumnMetadataReader}.
     *
     * @param connection          JDBC connection through which the column metadata is read from the remote database
     * @param properties          user-defined adapter properties
     * @param identifierConverter converter between source and Exasol identifiers
     */
    public BaseColumnMetadataReader(final Connection connection, final AdapterProperties properties,
            final IdentifierConverter identifierConverter) {
        super(connection, properties);
        this.identifierConverter = identifierConverter;
    }

    /**
     * Map a metadata for a list of columns to Exasol metadata.
     *
     * @param tableName the table for which the columns are mapped
     * @return list of Exasol column metadata objects
     */
    @Override
    public List<ColumnMetadata> mapColumns(final String tableName) {
        return mapColumns(getCatalogNameFilter(), getSchemaNameFilter(), tableName);
    }

    protected List<ColumnMetadata> mapColumns(final String catalogName, final String schemaName,
            final String tableName) {
        try (final ResultSet remoteColumns = this.connection.getMetaData().getColumns(catalogName, schemaName,
                tableName, ANY_COLUMN)) {
            return getColumnsFromResultSet(remoteColumns);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to read column metadata from remote for catalog \""
                    + catalogName + "\" and schema \"" + schemaName + "\"", exception);
        }
    }

    protected List<ColumnMetadata> getColumnsFromResultSet(final ResultSet remoteColumns) throws SQLException {
        final List<ColumnMetadata> columns = new ArrayList<>();
        while (remoteColumns.next()) {
            mapOrSkipColumn(remoteColumns, columns);
        }
        return columns;
    }

    public void mapOrSkipColumn(final ResultSet remoteColumns, final List<ColumnMetadata> columns) throws SQLException {
        final ColumnMetadata metadata = mapColumn(remoteColumns);
        if (metadata.getType().isSupported()) {
            columns.add(metadata);
        } else {
            LOGGER.fine(() -> "Column \"" + metadata.getName() + "\" of type \"" + metadata.getOriginalTypeName()
                    + "\" not supported by Virtual Schema. Skipping column in mapping.");
        }
    }

    private ColumnMetadata mapColumn(final ResultSet remoteColumn) throws SQLException {
        final JdbcTypeDescription jdbcTypeDescription = readJdbcTypeDescription(remoteColumn);
        final String columnName = readColumnName(remoteColumn);
        final String originalTypeName = readColumnTypeName(remoteColumn);
        final String adapterNotes = ColumnAdapterNotes
                .serialize(new ColumnAdapterNotes(jdbcTypeDescription.getJdbcType(), originalTypeName));
        final DataType exasolType = mapJdbcType(jdbcTypeDescription);
        return ColumnMetadata.builder() //
                .name(columnName) //
                .adapterNotes(adapterNotes) //
                .type(exasolType) //
                .nullable(isRemoteColumnNullable(remoteColumn, columnName)) //
                .identity(isAutoIncrementColumn(remoteColumn, columnName)) //
                .defaultValue(readDefaultValue(remoteColumn)) //
                .comment(readComment(remoteColumn)) //
                .originalTypeName(originalTypeName) //
                .build();
    }

    public JdbcTypeDescription readJdbcTypeDescription(final ResultSet remoteColumn) throws SQLException {
        final int jdbcType = readJdbcDataType(remoteColumn);
        final int decimalScale = readScale(remoteColumn);
        final int precisionOrSize = readPrecisionOrSize(remoteColumn);
        final int charOctetLength = readOctetLength(remoteColumn);
        final String typeName = readTypeName(remoteColumn);
        return new JdbcTypeDescription(jdbcType, decimalScale, precisionOrSize, charOctetLength, typeName);
    }

    private int readJdbcDataType(final ResultSet remoteColumn) throws SQLException {
        return remoteColumn.getInt(DATA_TYPE_COLUMN);
    }

    private int readScale(final ResultSet remoteColumn) throws SQLException {
        return remoteColumn.getInt(SCALE_COLUMN);
    }

    private int readPrecisionOrSize(final ResultSet remoteColumn) throws SQLException {
        return remoteColumn.getInt(SIZE_COLUMN);
    }

    private int readOctetLength(final ResultSet remoteColumn) throws SQLException {
        return remoteColumn.getInt(CHAR_OCTET_LENGTH_COLUMN);
    }

    private String readTypeName(final ResultSet remoteColumn) throws SQLException {
        return remoteColumn.getString(TYPE_NAME_COLUMN);
    }

    protected boolean isRemoteColumnNullable(final ResultSet remoteColumn, final String columnName) {
        try {
            return !JDBC_FALSE.equalsIgnoreCase(remoteColumn.getString(NULLABLE_COLUMN));
        } catch (final SQLException exception) {
            LOGGER.warning(() -> "Caught an SQL exception trying to determine whether column \"" + columnName
                    + "\" is nullable: " + exception.getMessage());
            LOGGER.warning(() -> "Assuming column \"" + columnName + "\" to be nullable.");
            return DEFAULT_NULLABLE;
        }
    }

    private boolean isAutoIncrementColumn(final ResultSet remoteColumn, final String columnName) {
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
        } catch (final SQLException exception) {
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
        } catch (final SQLException exception) {
            return "";
        }
    }

    private String readColumnTypeName(final ResultSet remoteColumn) throws SQLException {
        final String columnTypeName = readTypeName(remoteColumn);
        return (columnTypeName == null) ? "" : columnTypeName;
    }

    protected String readColumnName(final ResultSet columns) throws SQLException {
        return this.identifierConverter.convert(columns.getString(NAME_COLUMN));
    }

    @Override
    public DataType mapJdbcType(final JdbcTypeDescription jdbcTypeDescription) {
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.TINYINT:
        case Types.SMALLINT:
            return convertSmallInteger(jdbcTypeDescription.getPrecisionOrSize());
        case Types.INTEGER:
            return convertInteger(jdbcTypeDescription.getPrecisionOrSize());
        case Types.BIGINT:
            return convertBigInteger(jdbcTypeDescription.getPrecisionOrSize());
        case Types.DECIMAL:
            return convertDecimal(jdbcTypeDescription.getPrecisionOrSize(), jdbcTypeDescription.getDecimalScale());
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
            return DataType.createDouble();
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR:
            return convertVarChar(jdbcTypeDescription.getPrecisionOrSize(), jdbcTypeDescription.getCharOctetLength());
        case Types.CHAR:
        case Types.NCHAR:
            return convertChar(jdbcTypeDescription.getPrecisionOrSize(), jdbcTypeDescription.getCharOctetLength());
        case Types.DATE:
            return DataType.createDate();
        case Types.TIMESTAMP:
            return DataType.createTimestamp(false);
        case Types.BIT:
        case Types.BOOLEAN:
            return DataType.createBool();
        case Types.BINARY:
        case Types.CLOB:
        case Types.TIME:
        case Types.NUMERIC:
            return fallBackToMaximumSizeVarChar();
        case Types.OTHER:
        case Types.BLOB:
        case Types.NCLOB:
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
        case Types.JAVA_OBJECT:
        case Types.DISTINCT:
        case Types.STRUCT:
        case Types.ARRAY:
        case Types.REF:
        case Types.DATALINK:
        case Types.SQLXML:
        case Types.NULL:
        case Types.REF_CURSOR:
        default:
            LOGGER.finer("Found unsupported type: " + jdbcTypeDescription.getJdbcType());
            return DataType.createUnsupported();
        }
    }

    private static DataType convertSmallInteger(final int jdbcPrecision) {
        final DataType colType;
        final int precision = jdbcPrecision == 0 ? 9 : jdbcPrecision;
        colType = DataType.createDecimal(precision, 0);
        return colType;
    }

    private static DataType convertInteger(final int jdbcPrecision) {
        final DataType colType;
        if (jdbcPrecision <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
            final int precision = jdbcPrecision == 0 ? 18 : jdbcPrecision;
            colType = DataType.createDecimal(precision, 0);
        } else {
            colType = fallBackToMaximumSizeVarChar();
        }
        return colType;
    }

    private static DataType convertBigInteger(final int jdbcPrecision) {
        final DataType colType;
        if (jdbcPrecision <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
            final int precision = jdbcPrecision == 0 ? 36 : jdbcPrecision;
            colType = DataType.createDecimal(precision, 0);
        } else {
            colType = fallBackToMaximumSizeVarChar();
        }
        return colType;
    }

    protected DataType convertDecimal(final int jdbcPrecision, final int scale) {
        final DataType colType;
        if (jdbcPrecision <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
            colType = DataType.createDecimal(jdbcPrecision, scale);
        } else {
            colType = fallBackToMaximumSizeVarChar();
        }
        return colType;
    }

    private static DataType fallBackToMaximumSizeVarChar() {
        return DataType.createMaximumSizeVarChar(DataType.ExaCharset.UTF8);
    }

    private static DataType convertVarChar(final int size, final int octetLength) {
        final DataType colType;
        final DataType.ExaCharset charset = (octetLength == size) ? DataType.ExaCharset.ASCII
                : DataType.ExaCharset.UTF8;
        if (size <= DataType.MAX_EXASOL_VARCHAR_SIZE) {
            final int precision = size == 0 ? DataType.MAX_EXASOL_VARCHAR_SIZE : size;
            colType = DataType.createVarChar(precision, charset);
        } else {
            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, charset);
        }
        return colType;
    }

    private static DataType convertChar(final int size, final int octetLength) {
        final DataType colType;
        final DataType.ExaCharset charset = (octetLength == size) ? DataType.ExaCharset.ASCII
                : DataType.ExaCharset.UTF8;
        if (size <= DataType.MAX_EXASOL_CHAR_SIZE) {
            colType = DataType.createChar(size, charset);
        } else {
            if (size <= DataType.MAX_EXASOL_VARCHAR_SIZE) {
                colType = DataType.createVarChar(size, charset);
            } else {
                colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, charset);
            }
        }
        return colType;
    }

    /**
     * Map a <code>NUMERIC</code> column to an Exasol <code>DECIMAL</code>
     * <p>
     * If the precision of the remote column exceeds the maximum precision of an Exasol <code>DECIMAL</code>, the column
     * is mapped to an Exasol <code>DOUBLE</code> instead.
     *
     * @param jdbcTypeDescription parameter object describing the type from the JDBC perspective
     * @return Exasol <code>DECIMAL</code> if precision is less than or equal maximum precision, <code>DOUBLE</code>
     *         otherwise.
     */
    protected DataType mapJdbcTypeNumericToDecimalWithFallbackToDouble(final JdbcTypeDescription jdbcTypeDescription) {
        final int decimalPrec = jdbcTypeDescription.getPrecisionOrSize();
        final int decimalScale = jdbcTypeDescription.getDecimalScale();
        if (decimalPrec <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
            return DataType.createDecimal(decimalPrec, decimalScale);
        } else {
            return DataType.createDouble();
        }
    }

    protected DataType getNumberTypeFromProperty(final String property) {
        final Pattern pattern = Pattern.compile("\\s*(\\d+)\\s*,\\s*(\\d+)\\s*");
        final String precisionAndScale = this.properties.get(property);
        final Matcher matcher = pattern.matcher(precisionAndScale);
        if (matcher.matches()) {
            final int precision = Integer.parseInt(matcher.group(1));
            final int scale = Integer.parseInt(matcher.group(2));
            return DataType.createDecimal(precision, scale);
        } else {
            throw new IllegalArgumentException("Unable to parse adapter property " + property + " value \""
                    + precisionAndScale
                    + " into a number precision and scale. The required format is \"<precision>.<scale>\", where both are integer numbers.");
        }
    }
}