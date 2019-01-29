package com.exasol.adapter.dialects.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.EnumMap;
import java.util.Map;

import com.exasol.adapter.capabilities.AggregateFunctionCapability;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.LiteralCapability;
import com.exasol.adapter.capabilities.MainCapability;
import com.exasol.adapter.capabilities.PredicateCapability;
import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.AbstractSqlDialect;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

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
        final Capabilities cap = new Capabilities();

        // Capabilities
        cap.supportMainCapability(MainCapability.SELECTLIST_PROJECTION);
        cap.supportMainCapability(MainCapability.SELECTLIST_EXPRESSIONS);
        cap.supportMainCapability(MainCapability.FILTER_EXPRESSIONS);
        cap.supportMainCapability(MainCapability.AGGREGATE_SINGLE_GROUP);
        cap.supportMainCapability(MainCapability.AGGREGATE_GROUP_BY_COLUMN);
        cap.supportMainCapability(MainCapability.AGGREGATE_GROUP_BY_EXPRESSION);
        cap.supportMainCapability(MainCapability.AGGREGATE_GROUP_BY_TUPLE);
        cap.supportMainCapability(MainCapability.AGGREGATE_HAVING);
        cap.supportMainCapability(MainCapability.ORDER_BY_COLUMN);
        cap.supportMainCapability(MainCapability.ORDER_BY_EXPRESSION);
        cap.supportMainCapability(MainCapability.LIMIT);
        cap.supportMainCapability(MainCapability.LIMIT_WITH_OFFSET);

        // Predicates
        cap.supportPredicate(PredicateCapability.AND);
        cap.supportPredicate(PredicateCapability.OR);
        cap.supportPredicate(PredicateCapability.NOT);
        cap.supportPredicate(PredicateCapability.EQUAL);
        cap.supportPredicate(PredicateCapability.NOTEQUAL);
        cap.supportPredicate(PredicateCapability.LESS);
        cap.supportPredicate(PredicateCapability.LESSEQUAL);
        cap.supportPredicate(PredicateCapability.LIKE);
        cap.supportPredicate(PredicateCapability.LIKE_ESCAPE);
        cap.supportPredicate(PredicateCapability.REGEXP_LIKE);
        cap.supportPredicate(PredicateCapability.BETWEEN);
        cap.supportPredicate(PredicateCapability.IN_CONSTLIST);
        cap.supportPredicate(PredicateCapability.IS_NULL);
        cap.supportPredicate(PredicateCapability.IS_NOT_NULL);

        // Literals
        // BOOL is not supported
        cap.supportLiteral(LiteralCapability.NULL);
        cap.supportLiteral(LiteralCapability.DATE);
        cap.supportLiteral(LiteralCapability.TIMESTAMP);
        cap.supportLiteral(LiteralCapability.TIMESTAMP_UTC);
        cap.supportLiteral(LiteralCapability.DOUBLE);
        cap.supportLiteral(LiteralCapability.EXACTNUMERIC);
        cap.supportLiteral(LiteralCapability.STRING);
        cap.supportLiteral(LiteralCapability.INTERVAL);

        // Aggregate functions
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_STAR);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.GROUP_CONCAT);
        // GROUP_CONCAT_DISTINCT is not supported
        cap.supportAggregateFunction(AggregateFunctionCapability.GROUP_CONCAT_SEPARATOR);
        cap.supportAggregateFunction(AggregateFunctionCapability.GROUP_CONCAT_ORDER_BY);
        // GEO_INTERSECTION_AGGREGATE is not supported
        // GEO_UNION_AGGREGATE is not supported
        // APPROXIMATE_COUNT_DISTINCT supported with version >= 12.1.0.2
        if (this.castAggFuncToFloat) {
            // Cast result to FLOAT because result set precision = 0, scale = 0
            cap.supportAggregateFunction(AggregateFunctionCapability.SUM);
            cap.supportAggregateFunction(AggregateFunctionCapability.SUM_DISTINCT);
            cap.supportAggregateFunction(AggregateFunctionCapability.MIN);
            cap.supportAggregateFunction(AggregateFunctionCapability.MAX);
            cap.supportAggregateFunction(AggregateFunctionCapability.AVG);
            cap.supportAggregateFunction(AggregateFunctionCapability.AVG_DISTINCT);
            cap.supportAggregateFunction(AggregateFunctionCapability.MEDIAN);
            cap.supportAggregateFunction(AggregateFunctionCapability.FIRST_VALUE);
            cap.supportAggregateFunction(AggregateFunctionCapability.LAST_VALUE);
            cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV);
            cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_DISTINCT);
            cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_POP);
            // STDDEV_POP_DISTINCT
            cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_SAMP);
            // STDDEV_SAMP_DISTINCT
            cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE);
            cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE_DISTINCT);
            cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP);
            // VAR_POP_DISTINCT
            cap.supportAggregateFunction(AggregateFunctionCapability.VAR_SAMP);
            // VAR_SAMP_DISTINCT
        }

        // Scalar functions
        cap.supportScalarFunction(ScalarFunctionCapability.CEIL);
        cap.supportScalarFunction(ScalarFunctionCapability.DIV);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOOR);
        // ROUND is not supported. DATETIME could be pushed down, NUMBER would have to
        // be rounded.
        cap.supportScalarFunction(ScalarFunctionCapability.SIGN);
        // TRUNC is not supported. DATETIME could be pushed down, NUMBER would have to
        // be rounded.
        if (this.castScalarFuncToFloat) {
            // Cast result to FLOAT because result set precision = 0, scale = 0
            cap.supportScalarFunction(ScalarFunctionCapability.ADD);
            cap.supportScalarFunction(ScalarFunctionCapability.SUB);
            cap.supportScalarFunction(ScalarFunctionCapability.MULT);
            cap.supportScalarFunction(ScalarFunctionCapability.FLOAT_DIV);
            cap.supportScalarFunction(ScalarFunctionCapability.NEG);
            cap.supportScalarFunction(ScalarFunctionCapability.ABS);
            cap.supportScalarFunction(ScalarFunctionCapability.ACOS);
            cap.supportScalarFunction(ScalarFunctionCapability.ASIN);
            cap.supportScalarFunction(ScalarFunctionCapability.ATAN);
            cap.supportScalarFunction(ScalarFunctionCapability.ATAN2);
            cap.supportScalarFunction(ScalarFunctionCapability.COS);
            cap.supportScalarFunction(ScalarFunctionCapability.COSH);
            cap.supportScalarFunction(ScalarFunctionCapability.COT);
            cap.supportScalarFunction(ScalarFunctionCapability.DEGREES);
            cap.supportScalarFunction(ScalarFunctionCapability.EXP);
            cap.supportScalarFunction(ScalarFunctionCapability.GREATEST);
            cap.supportScalarFunction(ScalarFunctionCapability.LEAST);
            cap.supportScalarFunction(ScalarFunctionCapability.LN);
            cap.supportScalarFunction(ScalarFunctionCapability.LOG);
            cap.supportScalarFunction(ScalarFunctionCapability.MOD);
            cap.supportScalarFunction(ScalarFunctionCapability.POWER);
            cap.supportScalarFunction(ScalarFunctionCapability.RADIANS);
            // RAND is not supported (constant arguments in EXA, will not be pushed down)
            cap.supportScalarFunction(ScalarFunctionCapability.SIN);
            cap.supportScalarFunction(ScalarFunctionCapability.SINH);
            cap.supportScalarFunction(ScalarFunctionCapability.SQRT);
            cap.supportScalarFunction(ScalarFunctionCapability.TAN);
            cap.supportScalarFunction(ScalarFunctionCapability.TANH);
        }
        cap.supportScalarFunction(ScalarFunctionCapability.ASCII);
        // BIT_LENGTH is not supported. Can be different for Unicode characters.
        cap.supportScalarFunction(ScalarFunctionCapability.CHR);
        // COLOGNE_PHONETIC is not supported.
        // CONCAT is not supported. Number of arguments can be different.
        // DUMP is not supported. Output is different.
        // EDIT_DISTANCE is not supported. Output is different. UTL_MATCH.EDIT_DISTANCE
        // returns -1 with NULL argument.
        // INSERT is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.INSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.LENGTH);
        cap.supportScalarFunction(ScalarFunctionCapability.LOCATE);
        cap.supportScalarFunction(ScalarFunctionCapability.LOWER);
        cap.supportScalarFunction(ScalarFunctionCapability.LPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.LTRIM);
        // OCTET_LENGTH is not supported. Can be different for Unicode characters.
        cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_INSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_REPLACE);
        cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_SUBSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.REPEAT);
        cap.supportScalarFunction(ScalarFunctionCapability.REPLACE);
        cap.supportScalarFunction(ScalarFunctionCapability.REVERSE);
        // RIGHT is not supported. Possible solution with SUBSTRING (must handle corner
        // cases correctly).
        cap.supportScalarFunction(ScalarFunctionCapability.RPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.RTRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.SOUNDEX);
        // SPACE is not supported. Parameter = 0 has different results from RPAD.
        cap.supportScalarFunction(ScalarFunctionCapability.SUBSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.TRANSLATE);
        cap.supportScalarFunction(ScalarFunctionCapability.TRIM);
        // UNICODE is not supported.
        // UNICODECHR is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.UPPER);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_DAYS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_HOURS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MINUTES);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MONTHS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_SECONDS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_WEEKS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_YEARS);
        // CONVERT_TZ is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_DATE);
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_TIMESTAMP);
        // DATE_TRUNC is not supported. Format options for TRUNCATE are different.
        // DAY is not supported. EXTRACT does not work on strings.
        // DAYS_BETWEEN is not supported. EXTRACT does not work on strings.
        cap.supportScalarFunction(ScalarFunctionCapability.DBTIMEZONE);
        // EXTRACT is not supported. SECOND must be cast to DOUBLE.
        // HOURS_BETWEEN is not supported. EXTRACT does not work on strings.
        cap.supportScalarFunction(ScalarFunctionCapability.LOCALTIMESTAMP);
        // MINUTE is not supported. EXTRACT does not work on strings.
        // MINUTES_BETWEEN is not supported. EXTRACT does not work on strings.
        // MONTH is not supported. EXTRACT does not work on strings.
        // MONTHS_BETWEEN is not supported. EXTRACT does not work on strings.
        cap.supportScalarFunction(ScalarFunctionCapability.NUMTODSINTERVAL);
        cap.supportScalarFunction(ScalarFunctionCapability.NUMTOYMINTERVAL);
        // POSIX_TIME is not supported. Does not work on strings.
        // SECOND is not supported. EXTRACT does not work on strings.
        // SECONDS_BETWEEN is not supported. EXTRACT does not work on strings.
        cap.supportScalarFunction(ScalarFunctionCapability.SESSIONTIMEZONE);
        cap.supportScalarFunction(ScalarFunctionCapability.SYSDATE);
        cap.supportScalarFunction(ScalarFunctionCapability.SYSTIMESTAMP);
        // WEEK is not supported.
        // YEAR is not supported. EXTRACT does not work on strings.
        // YEARS_BETWEEN is not supported. EXTRACT does not work on strings.
        // ST_X is not supported.
        // ST_Y is not supported.
        // ST_ENDPOINT is not supported.
        // ST_ISCLOSED is not supported.
        // ST_ISRING is not supported.
        // ST_LENGTH is not supported.
        // ST_NUMPOINTS is not supported.
        // ST_POINTN is not supported.
        // ST_STARTPOINT is not supported.
        // ST_AREA is not supported.
        // ST_EXTERIORRING is not supported.
        // ST_INTERIORRINGN is not supported.
        // ST_NUMINTERIORRINGS is not supported.
        // ST_GEOMETRYN is not supported.
        // ST_NUMGEOMETRIES is not supported.
        // ST_BOUNDARY is not supported.
        // ST_BUFFER is not supported.
        // ST_CENTROID is not supported.
        // ST_CONTAINS is not supported.
        // ST_CONVEXHULL is not supported.
        // ST_CROSSES is not supported.
        // ST_DIFFERENCE is not supported.
        // ST_DIMENSION is not supported.
        // ST_DISJOINT is not supported.
        // ST_DISTANCE is not supported.
        // ST_ENVELOPE is not supported.
        // ST_EQUALS is not supported.
        // ST_FORCE2D is not supported.
        // ST_GEOMETRYTYPE is not supported.
        // ST_INTERSECTION is not supported.
        // ST_INTERSECTS is not supported.
        // ST_ISEMPTY is not supported.
        // ST_ISSIMPLE is not supported.
        // ST_OVERLAPS is not supported.
        // ST_SETSRID is not supported.
        // ST_SYMDIFFERENCE is not supported.
        // ST_TOUCHES is not supported.
        // ST_TRANSFORM is not supported.
        // ST_UNION is not supported.
        // ST_WITHIN is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.CAST);
        // IS_NUMBER is not supported.
        // IS_BOOLEAN is not supported.
        // IS_DATE is not supported.
        // IS_DSINTERVAL is not supported.
        // IS_YMINTERVAL is not supported.
        // IS_TIMESTAMP is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.TO_CHAR);
        cap.supportScalarFunction(ScalarFunctionCapability.TO_DATE);
        cap.supportScalarFunction(ScalarFunctionCapability.TO_DSINTERVAL);
        cap.supportScalarFunction(ScalarFunctionCapability.TO_YMINTERVAL);
        cap.supportScalarFunction(ScalarFunctionCapability.TO_NUMBER);
        cap.supportScalarFunction(ScalarFunctionCapability.TO_TIMESTAMP);
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_AND);
        // BIT_CHECK is not supported.
        // BIT_NOT is not supported.
        // BIT_OR is not supported.
        // BIT_SET is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_TO_NUM);
        // BIT_XOR is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.CASE);
        // CURRENT_SCHEMA is not supported.
        // CURRENT_SESSION is not supported.
        // CURRENT_STATEMENT is not supported.
        // CURRENT_USER is not supported.
        // HASH_MD5 is not supported.
        // HASH_SHA is not supported.
        // HASH_SHA1 is not supported.
        // HASH_TIGER is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.NULLIFZERO);
        // SYS_GUID is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.ZEROIFNULL);

        return cap;
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        final Map<AggregateFunction, String> aggregationAliases = new EnumMap<>(AggregateFunction.class);
        // APPROXIMATE_COUNT_DISTINCT supported with version >= 12.1.0.2
        // aggregationAliases.put(AggregateFunction.APPROXIMATE_COUNT_DISTINCT,
        // "APPROX_COUNT_DISTINCT");
        return aggregationAliases;
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
    public MappedTable mapTable(final ResultSet tables, String ignoreErrorList) throws SQLException {
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
            final int decimalPrec = jdbcTypeDescription.getPrecisionOrSize();
            final int decimalScale = jdbcTypeDescription.getDecimalScale();
            if (decimalScale == -127) {
                // Oracle JDBC driver returns scale -127 if NUMBER data type was specified
                // without scale and precision. Convert to VARCHAR.
                // See http://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#i16209
                // and https://docs.oracle.com/cd/E19501-01/819-3659/gcmaz/
                colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
                break;
            }
            if (decimalPrec <= DataType.maxExasolDecimalPrecision) {
                colType = DataType.createDecimal(decimalPrec, decimalScale);
            } else {
                colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
            }
            break;
        case Types.OTHER:
            // Oracle JDBC uses OTHER as CLOB
            colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
            break;
        case -103:
            // INTERVAL YEAR TO MONTH
        case -104:
            // INTERVAL DAY TO SECOND
            colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
            break;
        case -102:
        case -101:
            // -101 and -102 is TIMESTAMP WITH (LOCAL) TIMEZONE in Oracle.
            colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
            break;
        case 100:
        case 101:
            // 100 and 101 are BINARY_FLOAT and BINARY_DOUBLE in Oracle.
            colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
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

}
