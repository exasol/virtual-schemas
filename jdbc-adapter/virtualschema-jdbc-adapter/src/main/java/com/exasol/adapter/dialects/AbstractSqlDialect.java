package com.exasol.adapter.dialects;

import java.sql.*;
import java.util.*;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * Abstract implementation of a dialect. We recommend that every dialect should extend this abstract class.
 */
public abstract class AbstractSqlDialect implements SqlDialect {
    protected Set<ScalarFunction> omitParenthesesMap = new HashSet<>();
    protected RemoteMetadataReader remoteMetadataReader;
    protected AdapterProperties properties;
    protected final Connection connection;

    /**
     * Create a new instance of an {@link AbstractSqlDialect}
     *
     * @param properties user properties
     */
    public AbstractSqlDialect(final Connection connection, final AdapterProperties properties) {
        this.connection = connection;
        this.remoteMetadataReader = createRemoteDataReader();
        this.properties = properties;
    }

    /**
     * Create the {@link RemoteMetadataReader} that is used to get the database metadata from the remote source.
     * <p>
     * Override this method in the concrete SQL dialect implementation if the dialect requires non-standard metadata
     * mapping.
     *
     * @return metadata reader
     */
    protected RemoteMetadataReader createRemoteDataReader() {
        return new BaseRemoteMetadataReader(this.connection, this.properties);
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

    private static DataType getExaTypeFromJdbcType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        final DataType colType;
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
            final DataType.ExaCharset charset = (jdbcTypeDescription.getCharOctetLength() == jdbcTypeDescription
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
            final DataType.ExaCharset charset = (jdbcTypeDescription.getCharOctetLength() == jdbcTypeDescription
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

    @Override
    public void handleException(final SQLException exception,
            final JdbcAdapterProperties.ExceptionHandlingMode exceptionMode) throws SQLException {
        throw exception;
    }

    @Override
    public String generatePushdownSql(final ConnectionInformation connectionInformation, final String columnDescription,
            final String pushdownSql) {
        final StringBuilder jdbcImportQuery = new StringBuilder();
        if (columnDescription == null) {
            jdbcImportQuery.append("IMPORT FROM JDBC AT ").append(connectionInformation.getCredentials());
        } else {
            jdbcImportQuery.append("IMPORT INTO ").append(columnDescription);
            jdbcImportQuery.append(" FROM JDBC AT ").append(connectionInformation.getCredentials());
        }
        jdbcImportQuery.append(" STATEMENT '").append(pushdownSql.replace("'", "''")).append("'");
        return jdbcImportQuery.toString();
    }

    @Override
    public SchemaMetadata readSchemaMetadata(final List<String> whiteListedRemoteTables) {
        return this.remoteMetadataReader.readRemoteSchemaMetadata(); // FIXME: use table white list
    }
}