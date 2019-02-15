package com.exasol.adapter.dialects;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.jdbc.JdbcAdapterProperties;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * Abstract implementation of a dialect. We recommend that every dialect should
 * extend this abstract class.
 *
 * TODO Find solution to handle unsupported types (e.g. exceeding varchar size).
 * E.g. skip column or always truncate or add const-null column or throw error
 * or make configurable
 */
public abstract class AbstractSqlDialect implements SqlDialect {

    protected Set<ScalarFunction> omitParenthesesMap = new HashSet<>();

    private final SqlDialectContext context;

    public AbstractSqlDialect(final SqlDialectContext context) {
        this.context = context;
    }

    @Override
    public String getTableCatalogAndSchemaSeparator() {
        return ".";
    }

    @Override
    public MappedTable mapTable(final ResultSet tables, final List<String> ignoreErrorList) throws SQLException {
        String commentString = tables.getString("REMARKS");
        if (commentString == null) {
            commentString = "";
        }
        final String tableName = changeIdentifierCaseIfNeeded(tables.getString("TABLE_NAME"));
        return MappedTable.createMappedTable(tableName, tables.getString("TABLE_NAME"), commentString);
    }

    @Override
    public ColumnMetadata mapColumn(final ResultSet columns) throws SQLException {
        final String colName = changeIdentifierCaseIfNeeded(columns.getString("COLUMN_NAME"));
        final int jdbcType = columns.getInt("DATA_TYPE");
        final int decimalScale = columns.getInt("DECIMAL_DIGITS");
        final int precisionOrSize = columns.getInt("COLUMN_SIZE");
        final int charOctedLength = columns.getInt("CHAR_OCTET_LENGTH");
        final String typeName = columns.getString("TYPE_NAME");
        final JdbcTypeDescription jdbcTypeDescription = new JdbcTypeDescription(jdbcType, decimalScale, precisionOrSize,
                charOctedLength, typeName);
        // Check if dialect want's to handle this row
        final DataType colType = mapJdbcType(jdbcTypeDescription);

        // Nullable
        boolean isNullable = true;
        try {
            final String nullable = columns.getString("IS_NULLABLE");
            if (nullable != null && nullable.toLowerCase().equals("no")) {
                isNullable = false;
            }
        } catch (final SQLException ex) {
            // ignore me
        }

        // Identity

        boolean isIdentity = false;
        try {
            final String identity = columns.getString("IS_AUTOINCREMENT");
            if (identity != null && identity.toLowerCase().equals("yes")) {
                isIdentity = true;
            }
        } catch (final SQLException ex) {
            // ignore me --some older JDBC drivers (Java 1.5) don't support IS_AUTOINCREMENT
        }

        // Default
        String defaultValue = "";
        try {
            final String defaultString = columns.getString("COLUMN_DEF");
            if (defaultString != null) {
                defaultValue = defaultString;
            }
        } catch (final SQLException ex) {
            // ignore me
        }

        // Comment
        String comment = "";
        try {
            final String commentString = columns.getString("REMARKS");
            if (commentString != null && !commentString.isEmpty()) {
                comment = commentString;
            }
        } catch (final SQLException ex) {
            // ignore me
        }

        // Column type
        String columnTypeName = columns.getString("TYPE_NAME");
        if (columnTypeName == null) {
            columnTypeName = "";
        }
        final String adapterNotes = ColumnAdapterNotes.serialize(new ColumnAdapterNotes(jdbcType, columnTypeName));
        ;
        return new ColumnMetadata(colName, adapterNotes, colType, isNullable, isIdentity, defaultValue, comment);
    }

    private static DataType getExaTypeFromJdbcType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        DataType colType;
        switch (jdbcTypeDescription.getJdbcType()) {
        case Types.TINYINT:
        case Types.SMALLINT:
            if (jdbcTypeDescription.getPrecisionOrSize() <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
                final int precision = jdbcTypeDescription.getPrecisionOrSize() == 0 ? 9
                        : jdbcTypeDescription.getPrecisionOrSize();
                colType = DataType.createDecimal(precision, 0);
            } else {
                colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            }
            break;
        case Types.INTEGER:
            if (jdbcTypeDescription.getPrecisionOrSize() <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
                final int precision = jdbcTypeDescription.getPrecisionOrSize() == 0 ? 18
                        : jdbcTypeDescription.getPrecisionOrSize();
                colType = DataType.createDecimal(precision, 0);
            } else {
                colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            }
            break;
        case Types.BIGINT: // Java type long
            if (jdbcTypeDescription.getPrecisionOrSize() <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
                final int precision = jdbcTypeDescription.getPrecisionOrSize() == 0 ? 36
                        : jdbcTypeDescription.getPrecisionOrSize();
                colType = DataType.createDecimal(precision, 0);
            } else {
                colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            }
            break;
        case Types.DECIMAL:

            if (jdbcTypeDescription.getPrecisionOrSize() <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
                colType = DataType.createDecimal(jdbcTypeDescription.getPrecisionOrSize(),
                        jdbcTypeDescription.getDecimalScale());
            } else {
                colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            }
            break;
        case Types.NUMERIC: // Java BigInteger
            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            break;
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
            colType = DataType.createDouble();
            break;
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR: {
            final DataType.ExaCharset charset = (jdbcTypeDescription.getCharOctedLength() == jdbcTypeDescription
                    .getPrecisionOrSize()) ? DataType.ExaCharset.ASCII : DataType.ExaCharset.UTF8;
            if (jdbcTypeDescription.getPrecisionOrSize() <= DataType.MAX_EXASOL_VARCHAR_SIZE) {
                final int precision = jdbcTypeDescription.getPrecisionOrSize() == 0 ? DataType.MAX_EXASOL_VARCHAR_SIZE
                        : jdbcTypeDescription.getPrecisionOrSize();
                colType = DataType.createVarChar(precision, charset);
            } else {
                colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, charset);
            }
            break;
        }
        case Types.CHAR:
        case Types.NCHAR: {
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
            break;
        }
        case Types.DATE:
            colType = DataType.createDate();
            break;
        case Types.TIMESTAMP:
            colType = DataType.createTimestamp(false);
            break;
        case Types.TIME:
            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            break;
        case Types.BIT:
        case Types.BOOLEAN:
            colType = DataType.createBool();
            break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BLOB:
        case Types.CLOB:
        case Types.NCLOB:
            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            break;
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
            throw new RuntimeException("Unsupported data type (" + jdbcTypeDescription.getJdbcType()
                    + ") found in source system, should never happen");
        }
        assert (colType != null);
        return colType;
    }

    public String changeIdentifierCaseIfNeeded(final String identifier) {
        if (getQuotedIdentifierHandling() == getUnquotedIdentifierHandling()) {
            if (getQuotedIdentifierHandling() != IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE) {
                // Completely case-insensitive. We can store everything uppercase to allow
                // working with unquoted identifiers in EXASOL
                return identifier.toUpperCase();
            }
        }
        return identifier;
    }

    @Override
    public boolean omitParentheses(final ScalarFunction function) {
        return this.omitParenthesesMap.contains(function);
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new SqlGenerationVisitor(this, context);
    }

    @Override
    public abstract DataType dialectSpecificMapJdbcType(JdbcTypeDescription jdbcType) throws SQLException;

    @Override
    public final DataType mapJdbcType(final JdbcTypeDescription jdbcType) throws SQLException {
        DataType type = dialectSpecificMapJdbcType(jdbcType);
        if (type == null) {
            type = getExaTypeFromJdbcType(jdbcType);
        }
        return type;
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        return new EnumMap<>(ScalarFunction.class);
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        final Map<AggregateFunction, String> aliases = new HashMap<>();
        aliases.put(AggregateFunction.GEO_INTERSECTION_AGGREGATE, "ST_INTERSECTION");
        aliases.put(AggregateFunction.GEO_UNION_AGGREGATE, "ST_UNION");
        return aliases;
    }

    @Override
    public Map<ScalarFunction, String> getBinaryInfixFunctionAliases() {
        final Map<ScalarFunction, String> aliases = new HashMap<>();
        aliases.put(ScalarFunction.ADD, "+");
        aliases.put(ScalarFunction.SUB, "-");
        aliases.put(ScalarFunction.MULT, "*");
        aliases.put(ScalarFunction.FLOAT_DIV, "/");
        return aliases;
    }

    @Override
    public Map<ScalarFunction, String> getPrefixFunctionAliases() {
        final Map<ScalarFunction, String> aliases = new HashMap<>();
        aliases.put(ScalarFunction.NEG, "-");
        return aliases;
    }

    public SqlDialectContext getContext() {
        return this.context;
    }

    @Override
    public void handleException(final SQLException exception,
            final JdbcAdapterProperties.ExceptionHandlingMode exceptionMode) throws SQLException {
        throw exception;
    };
}
