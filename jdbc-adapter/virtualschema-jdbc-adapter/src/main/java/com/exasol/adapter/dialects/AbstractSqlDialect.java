package com.exasol.adapter.dialects;

import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

/**
 * Abstract implementation of a dialect. We recommend that every dialect should extend this abstract class.
 *
 * TODO Find solution to handle unsupported types (e.g. exceeding varchar size). E.g. skip column or always truncate or add const-null column or throw error or make configurable
 */
public abstract class AbstractSqlDialect implements SqlDialect {

    protected Set<ScalarFunction> omitParenthesesMap = new HashSet<>();

    private SqlDialectContext context;

    public AbstractSqlDialect(SqlDialectContext context) {
        this.context = context;
    }

    @Override
    public String getTableCatalogAndSchemaSeparator() {
        return ".";
    }

    @Override
    public MappedTable mapTable(ResultSet tables) throws SQLException {
//        for (int i=1; i<=tables.getMetaData().getColumnCount(); ++i) {
//            System.out.println("  - " + tables.getMetaData().getColumnName(i) + ": " + tables.getString(i));
//        }
        String commentString = tables.getString("REMARKS");
        if (commentString == null) {
            commentString = "";
        }
        String tableName = changeIdentifierCaseIfNeeded(tables.getString("TABLE_NAME"));
        return MappedTable.createMappedTable(tableName,tables.getString("TABLE_NAME"), commentString);
    }

    @Override
    public ColumnMetadata mapColumn(ResultSet columns) throws SQLException {
        String colName = changeIdentifierCaseIfNeeded(columns.getString("COLUMN_NAME"));
        int jdbcType = columns.getInt("DATA_TYPE");
        // Check if dialect want's to handle this row
        DataType colType = mapJdbcType(columns);
        if (colType == null) {
            colType = defaultJdbcTypeMapping(columns);
        }

        // Nullable
        boolean isNullable = true;
        try {
            String nullable = columns.getString("IS_NULLABLE");
            if (nullable != null && nullable.toLowerCase().equals("no")) {
                isNullable = false;
            }
        } catch (SQLException ex) {
            // ignore me
        }

        // Identity
        
        boolean isIdentity = false;
        try {
            String identity = columns.getString("IS_AUTOINCREMENT");
            if (identity != null && identity.toLowerCase().equals("yes")) {
                isIdentity = true;
            }
        } catch (SQLException ex) {
            // ignore me --some older JDBC drivers (Java 1.5) don't support IS_AUTOINCREMENT
        }

        // Default
        String defaultValue = "";
        try {
            String defaultString = columns.getString("COLUMN_DEF");
            if (defaultString != null) {
                defaultValue = defaultString;
            }
        } catch (SQLException ex) {
            // ignore me
        }

        // Comment
        String comment = "";
        try {
            String commentString = columns.getString("REMARKS");
            if (commentString != null && !commentString.isEmpty()) {
                comment = commentString;
            }
        } catch (SQLException ex) {
            // ignore me
        }

        // Column type
        String columnTypeName = columns.getString("TYPE_NAME");
        if (columnTypeName == null) {
            columnTypeName = "";
        }
        String adapterNotes = ColumnAdapterNotes.serialize(new ColumnAdapterNotes(jdbcType, columnTypeName));;
        return new ColumnMetadata(colName, adapterNotes, colType, isNullable, isIdentity, defaultValue, comment);
    }

    private static DataType defaultJdbcTypeMapping(ResultSet cols) throws SQLException {

        DataType colType;
        int jdbcType = cols.getInt("DATA_TYPE");
        switch (jdbcType) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:  // Java type long
                int intPrec = cols.getInt("COLUMN_SIZE");
                if (intPrec <= DataType.maxExasolDecimalPrecision) {
                    colType = DataType.createDecimal(intPrec, 0);
                } else {
                    colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
                }
                break;
            case Types.DECIMAL:
                int decimalPrec = cols.getInt("COLUMN_SIZE");
                int decimalScale = cols.getInt("DECIMAL_DIGITS");
                if (decimalPrec <= DataType.maxExasolDecimalPrecision) {
                    colType = DataType.createDecimal(decimalPrec, decimalScale);
                } else {
                    colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
                }
                break;
            case Types.NUMERIC: // Java BigInteger
                colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
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
                int size = cols.getInt("COLUMN_SIZE");
                DataType.ExaCharset charset = (cols.getInt("CHAR_OCTET_LENGTH") == size) ? DataType.ExaCharset.ASCII : DataType.ExaCharset.UTF8;
                if (size <= DataType.maxExasolVarcharSize) {
                    colType = DataType.createVarChar(size, charset);
                } else {
                    colType = DataType.createVarChar(DataType.maxExasolVarcharSize, charset);
                }
                break;
            }
            case Types.CHAR:
            case Types.NCHAR: {
                int size = cols.getInt("COLUMN_SIZE");
                DataType.ExaCharset charset = (cols.getInt("CHAR_OCTET_LENGTH") == size) ? DataType.ExaCharset.ASCII : DataType.ExaCharset.UTF8;
                if (size <= DataType.maxExasolCharSize) {
                    colType = DataType.createChar(size, charset);
                } else {
                    if (size <= DataType.maxExasolVarcharSize) {
                        colType = DataType.createVarChar(size, charset);
                    } else {
                        colType = DataType.createVarChar(DataType.maxExasolVarcharSize, charset);
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
                colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
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
                colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
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
                throw new RuntimeException("Unsupported data type (" + jdbcType + ") found in source system, should never happen");
        }
        assert(colType != null);
        return colType;
    }

    public String changeIdentifierCaseIfNeeded(String identifier) {
        if (getQuotedIdentifierHandling() == getUnquotedIdentifierHandling()) {
            if (getQuotedIdentifierHandling() != IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE) {
                // Completely case-insensitive. We can store everything uppercase to allow working with unquoted identifiers in EXASOL
                return identifier.toUpperCase();
            }
        }
        return identifier;
    }

    @Override
    public boolean omitParentheses(ScalarFunction function) {
        return omitParenthesesMap.contains(function);
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(SqlGenerationContext context) {
        return new SqlGenerationVisitor(this, context);
    }

    @Override
    public DataType mapJdbcType(ResultSet cols) throws SQLException {
        return null;
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        return new EnumMap<>(ScalarFunction.class);
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        Map<AggregateFunction, String> aliases = new HashMap<>();
        aliases.put(AggregateFunction.GEO_INTERSECTION_AGGREGATE, "ST_INTERSECTION");
        aliases.put(AggregateFunction.GEO_UNION_AGGREGATE, "ST_UNION");
        return aliases;
    }

    @Override
    public Map<ScalarFunction, String> getBinaryInfixFunctionAliases() {
        Map<ScalarFunction, String> aliases = new HashMap<>();
        aliases.put(ScalarFunction.ADD, "+");
        aliases.put(ScalarFunction.SUB, "-");
        aliases.put(ScalarFunction.MULT, "*");
        aliases.put(ScalarFunction.FLOAT_DIV, "/");
        return aliases;
    }

    @Override
    public Map<ScalarFunction, String> getPrefixFunctionAliases() {
        Map<ScalarFunction, String> aliases = new HashMap<>();
        aliases.put(ScalarFunction.NEG, "-");
        return aliases;
    }

    public SqlDialectContext getContext() {
        return context;
    }
}
