package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionInformation;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

/**
 * Work in Progress
 */
public class OracleSqlDialect extends AbstractSqlDialect {
    private final boolean castAggFuncToFloat = true;
    private final boolean castScalarFuncToFloat = true;

    public OracleSqlDialect(final SqlDialectContext context) {
        super(context);
        this.omitParenthesesMap.add(ScalarFunction.SYSDATE);
        this.omitParenthesesMap.add(ScalarFunction.SYSTIMESTAMP);
    }

    private static final String NAME = "ORACLE";

    public static String getPublicName() {
        return NAME;
    }

    public boolean getCastAggFuncToFloat() {
        return this.castAggFuncToFloat;
    }

    public boolean getCastScalarFuncToFloat() {
        return this.castScalarFuncToFloat;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        builder.addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
              AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING,
              ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET);
        builder.addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, REGEXP_LIKE, BETWEEN,
              IN_CONSTLIST, IS_NULL, IS_NOT_NULL);
        builder.addLiteral(NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL);
        builder.addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, GROUP_CONCAT, GROUP_CONCAT_SEPARATOR,
              GROUP_CONCAT_ORDER_BY);
        if (this.castAggFuncToFloat) {
            builder.addAggregateFunction(SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT, MEDIAN, FIRST_VALUE, //
                  LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_SAMP, VARIANCE, VARIANCE_DISTINCT, VAR_POP,
                  VAR_SAMP);
        }
        builder.addScalarFunction(CEIL, DIV, FLOOR, SIGN);
        if (this.castScalarFuncToFloat) {
            builder.addScalarFunction(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, COS, COSH, COT,
                  DEGREES, EXP, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, SIN, SINH, SQRT, TAN, TANH);
        }
        builder.addScalarFunction(ASCII, CHR, INSTR, LENGTH, LOCATE, LOWER, LPAD, LTRIM, REGEXP_INSTR, REGEXP_REPLACE,
              REGEXP_SUBSTR, REPEAT, REPLACE, REVERSE, RPAD, RTRIM, SOUNDEX, SUBSTR, TRANSLATE, TRIM, UPPER, ADD_DAYS,
              ADD_HOURS, ADD_MINUTES, ADD_MONTHS, ADD_SECONDS, ADD_WEEKS, ADD_YEARS, CURRENT_DATE, CURRENT_TIMESTAMP,
              DBTIMEZONE, LOCALTIMESTAMP, NUMTODSINTERVAL, NUMTOYMINTERVAL, SESSIONTIMEZONE, SYSDATE, SYSTIMESTAMP,
              CAST, TO_CHAR, TO_DATE, TO_DSINTERVAL, TO_YMINTERVAL, TO_NUMBER, TO_TIMESTAMP, BIT_AND, BIT_TO_NUM, CASE,
              NULLIFZERO, ZEROIFNULL);
        return builder.build();
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        // APPROXIMATE_COUNT_DISTINCT supported with version >= 12.1.0.2
        // aggregationAliases.put(AggregateFunction.APPROXIMATE_COUNT_DISTINCT,
        // "APPROX_COUNT_DISTINCT");
        return new EnumMap<>(AggregateFunction.class);
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.UNSUPPORTED;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    @Override
    public MappedTable mapTable(final ResultSet tables, final List<String> ignoreErrorList) throws SQLException {
        final String tableName = tables.getString("TABLE_NAME");
        if (tableName.startsWith("BIN$")) {
            // In case of Oracle we may see deleted tables with strange names
            // (BIN$OeQco6jg/drgUDAKzmRzgA==$0). Should be filtered out. Squirrel also
            // doesn't see them for unknown reasons. See
            // http://stackoverflow.com/questions/2446053/what-are-the-bin-tables-in-oracles-all-tab-columns-table
            System.out.println("Skip table: " + tableName);
            return MappedTable.createIgnoredTable();
        } else {
            return super.mapTable(tables, ignoreErrorList);
        }
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        DataType colType = null;
        final int jdbcType = jdbcTypeDescription.getJdbcType();
        switch (jdbcType) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            final int decimalPrec = jdbcTypeDescription.getPrecisionOrSize();
            final int decimalScale = jdbcTypeDescription.getDecimalScale();
            if (decimalScale == -127) {
                // Oracle JDBC driver returns scale -127 if NUMBER data type was specified
                // without scale and precision. Convert to VARCHAR.
                // See http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#i16209
                // and https://docs.oracle.com/cd/E19501-01/819-3659/gcmaz/
                colType = getContext().getOracleCastNumberToDecimal();
                break;
            }
            if (decimalPrec <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
                colType = DataType.createDecimal(decimalPrec, decimalScale);
            } else {
                colType = getContext().getOracleCastNumberToDecimal();
            }
            break;
        case Types.OTHER:
            // Oracle JDBC uses OTHER as CLOB
            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            break;
        case -103:
            // INTERVAL YEAR TO MONTH
        case -104:
            // INTERVAL DAY TO SECOND
            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            break;
        case -102:
        case -101:
            // -101 and -102 is TIMESTAMP WITH (LOCAL) TIMEZONE in Oracle.
            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            break;
        case 100:
        case 101:
            // 100 and 101 are BINARY_FLOAT and BINARY_DOUBLE in Oracle.
            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            break;
        }
        return colType;
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new OracleSqlGenerationVisitor(this, context);
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_UPPER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }

    @Override
    public String applyQuote(final String identifier) {
        // If identifier contains double quotation marks ", it needs to be escaped by
        // another double quotation mark. E.g. "a""b" is the identifier a"b in the db.
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String applyQuoteIfNeeded(final String identifier) {
        // This is a simplified rule, which quotes all identifiers although not needed
        return applyQuote(identifier);
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_HIGH;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public String generatePushdownSql(final ConnectionInformation connectionInformation, final String columnDescription, final String pushdownSql) {
        final ImportType importType = getContext().getImportType();
        if (importType == ImportType.JDBC) {
            return super.generatePushdownSql(connectionInformation, columnDescription, pushdownSql);
        } else {
            if ((importType != ImportType.ORA)) {
                throw new AssertionError("OracleSqlDialect has wrong ImportType");
            }
            final StringBuilder oracleImportQuery = new StringBuilder();
            oracleImportQuery.append("IMPORT FROM ORA AT ").append(connectionInformation.getOraConnectionName()).append(" ");
            oracleImportQuery.append(connectionInformation.getCredentials());
            oracleImportQuery.append(" STATEMENT '").append(pushdownSql.replace("'", "''")).append("'");
            return oracleImportQuery.toString();
        }
    }

}
