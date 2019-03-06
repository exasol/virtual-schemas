package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

import java.sql.SQLException;
import java.sql.Types;
import java.util.EnumMap;
import java.util.Map;

public class SybaseSqlDialect extends AbstractSqlDialect {
    // The Sybase dialect started as a copy of the SQL Server dialect.
    // Tested Sybase version: ASE 16.0
    // Tested JDBC drivers: jtds-1.3.1 (https://sourceforge.net/projects/jtds/)
    // Documentation:
    // http://infocenter.sybase.com/help/index.jsp?topic=/com.sybase.infocenter.help.ase.16.0/doc/html/title.html
    // https://help.sap.com/viewer/p/SAP_ASE
    public final static int maxSybaseVarcharSize = 8000;
    public final static int maxSybaseNVarcharSize = 4000;
    private static final String NAME = "SYBASE";

    public SybaseSqlDialect(final SqlDialectContext context) {
        super(context);
    }

    public static String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities cap = new Capabilities();

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

        cap.supportLiteral(LiteralCapability.BOOL);
        cap.supportLiteral(LiteralCapability.NULL);
        cap.supportLiteral(LiteralCapability.DATE);
        cap.supportLiteral(LiteralCapability.TIMESTAMP);
        cap.supportLiteral(LiteralCapability.TIMESTAMP_UTC);
        cap.supportLiteral(LiteralCapability.DOUBLE);
        cap.supportLiteral(LiteralCapability.EXACTNUMERIC);
        cap.supportLiteral(LiteralCapability.STRING);
        cap.supportLiteral(LiteralCapability.INTERVAL);

        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_STAR);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_DISTINCT);

        cap.supportAggregateFunction(AggregateFunctionCapability.SUM); // works
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
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_POP_DISTINCT);

        cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE);
        cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE_DISTINCT);

        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP_DISTINCT);

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
        cap.supportScalarFunction(ScalarFunctionCapability.CEIL);
        cap.supportScalarFunction(ScalarFunctionCapability.COS);
        cap.supportScalarFunction(ScalarFunctionCapability.COT);
        cap.supportScalarFunction(ScalarFunctionCapability.DEGREES);
        cap.supportScalarFunction(ScalarFunctionCapability.EXP);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOOR);
        cap.supportScalarFunction(ScalarFunctionCapability.LOG);
        cap.supportScalarFunction(ScalarFunctionCapability.MOD);
        cap.supportScalarFunction(ScalarFunctionCapability.POWER);
        cap.supportScalarFunction(ScalarFunctionCapability.RADIANS);
        cap.supportScalarFunction(ScalarFunctionCapability.RAND);
        cap.supportScalarFunction(ScalarFunctionCapability.ROUND);
        cap.supportScalarFunction(ScalarFunctionCapability.SIGN);
        cap.supportScalarFunction(ScalarFunctionCapability.SIN);
        cap.supportScalarFunction(ScalarFunctionCapability.SQRT);
        cap.supportScalarFunction(ScalarFunctionCapability.TAN);
        cap.supportScalarFunction(ScalarFunctionCapability.TRUNC);
        cap.supportScalarFunction(ScalarFunctionCapability.ASCII);
        cap.supportScalarFunction(ScalarFunctionCapability.CHR);
        cap.supportScalarFunction(ScalarFunctionCapability.CONCAT);
        cap.supportScalarFunction(ScalarFunctionCapability.INSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.LENGTH);
        cap.supportScalarFunction(ScalarFunctionCapability.LOCATE);
        cap.supportScalarFunction(ScalarFunctionCapability.LOWER);
        cap.supportScalarFunction(ScalarFunctionCapability.LPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.LTRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.REPEAT);
        cap.supportScalarFunction(ScalarFunctionCapability.REPLACE);
        cap.supportScalarFunction(ScalarFunctionCapability.REVERSE);
        cap.supportScalarFunction(ScalarFunctionCapability.RIGHT);
        cap.supportScalarFunction(ScalarFunctionCapability.RPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.RTRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.SOUNDEX);
        cap.supportScalarFunction(ScalarFunctionCapability.SPACE);
        cap.supportScalarFunction(ScalarFunctionCapability.SUBSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.TRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.UNICODE);
        cap.supportScalarFunction(ScalarFunctionCapability.UPPER);

        cap.supportScalarFunction(ScalarFunctionCapability.ADD_DAYS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_HOURS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MINUTES);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MONTHS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_SECONDS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_WEEKS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_YEARS);

        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_DATE);
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_TIMESTAMP);

        cap.supportScalarFunction(ScalarFunctionCapability.DAY);

        cap.supportScalarFunction(ScalarFunctionCapability.SECONDS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.MINUTES_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.HOURS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.DAYS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.MONTHS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.YEARS_BETWEEN);

        cap.supportScalarFunction(ScalarFunctionCapability.MONTH);

        cap.supportScalarFunction(ScalarFunctionCapability.SYSDATE);
        cap.supportScalarFunction(ScalarFunctionCapability.SYSTIMESTAMP);

        cap.supportScalarFunction(ScalarFunctionCapability.YEAR);

        cap.supportScalarFunction(ScalarFunctionCapability.ST_X);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_Y);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ENDPOINT);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ISCLOSED);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ISRING);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_LENGTH);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_NUMPOINTS);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_POINTN);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_STARTPOINT);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_AREA);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_EXTERIORRING);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_INTERIORRINGN);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_NUMINTERIORRINGS);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_GEOMETRYN);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_NUMGEOMETRIES);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_BOUNDARY);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_BUFFER);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_CENTROID);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_CONTAINS);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_CONVEXHULL);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_CROSSES);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_DIFFERENCE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_DIMENSION);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_DISJOINT);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_DISTANCE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ENVELOPE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_EQUALS);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_GEOMETRYTYPE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_INTERSECTION);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_INTERSECTS);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ISEMPTY);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ISSIMPLE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_OVERLAPS);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_SYMDIFFERENCE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_TOUCHES);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_UNION);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_WITHIN);

        cap.supportScalarFunction(ScalarFunctionCapability.BIT_AND);
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_NOT);
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_OR);
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_XOR);

        cap.supportScalarFunction(ScalarFunctionCapability.CASE);
        cap.supportScalarFunction(ScalarFunctionCapability.HASH_MD5);
        cap.supportScalarFunction(ScalarFunctionCapability.HASH_SHA);
        cap.supportScalarFunction(ScalarFunctionCapability.HASH_SHA1);
        cap.supportScalarFunction(ScalarFunctionCapability.NULLIFZERO);
        cap.supportScalarFunction(ScalarFunctionCapability.ZEROIFNULL);
        return cap;
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        DataType colType = null;
        final int jdbcType = jdbcTypeDescription.getJdbcType();
        final String columnTypeName = jdbcTypeDescription.getTypeName();

        switch (jdbcType) {
        case Types.VARCHAR: // the JTDS JDBC Type for date, time, datetime2, datetimeoffset is 12
            if (columnTypeName.equalsIgnoreCase("date")) {
                colType = DataType.createDate();
            } else if (columnTypeName.equalsIgnoreCase("datetime2")) {
                colType = DataType.createTimestamp(false);
            }
            break;
        case Types.TIME:
            colType = DataType.createVarChar(210, DataType.ExaCharset.UTF8);
            break;
        case 2013: // Types.TIME_WITH_TIMEZONE is Java 1.8 specific
            colType = DataType.createVarChar(21, DataType.ExaCharset.UTF8);
            break;
        case Types.DATE:
            colType = DataType.createDate();
            break;
        case Types.NUMERIC:
        case Types.DECIMAL:
            final int decimalPrec = jdbcTypeDescription.getPrecisionOrSize();
            final int decimalScale = jdbcTypeDescription.getDecimalScale();

            if (decimalPrec <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
                colType = DataType.createDecimal(decimalPrec, decimalScale);
            } else {
                int size = decimalPrec + 1;
                if (decimalScale > 0) {
                    size++;
                }
                colType = DataType.createVarChar(size, DataType.ExaCharset.UTF8);
            }
            break;
        case Types.OTHER:

            // TODO
            colType = DataType.createVarChar(SybaseSqlDialect.maxSybaseVarcharSize, DataType.ExaCharset.UTF8);
            break;

        case Types.SQLXML:

            colType = DataType.createVarChar(SybaseSqlDialect.maxSybaseVarcharSize, DataType.ExaCharset.UTF8);
            break;

        case Types.CLOB: // TEXT and UNITEXT types in Sybase

            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            break;

        case Types.BLOB:
            if (columnTypeName.equalsIgnoreCase("hierarchyid")) {
                colType = DataType.createVarChar(4000, DataType.ExaCharset.UTF8);
            }
            if (columnTypeName.equalsIgnoreCase("geometry")) {
                colType = DataType.createVarChar(SybaseSqlDialect.maxSybaseVarcharSize, DataType.ExaCharset.UTF8);
            } else {
                colType = DataType.createVarChar(100, DataType.ExaCharset.UTF8);
            }
            break;
        case Types.DISTINCT:
            colType = DataType.createVarChar(100, DataType.ExaCharset.UTF8);
            break;
        }
        return colType;
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.ATAN2, "ATN2");
        scalarAliases.put(ScalarFunction.CEIL, "CEILING");
        scalarAliases.put(ScalarFunction.CHR, "CHAR");
        scalarAliases.put(ScalarFunction.LENGTH, "LEN");
        scalarAliases.put(ScalarFunction.LOCATE, "CHARINDEX");
        scalarAliases.put(ScalarFunction.REPEAT, "REPLICATE");
        scalarAliases.put(ScalarFunction.SUBSTR, "SUBSTRING");
        scalarAliases.put(ScalarFunction.NULLIFZERO, "NULLIF");
        return scalarAliases;
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        final Map<AggregateFunction, String> aggregationAliases = new EnumMap<>(AggregateFunction.class);
        aggregationAliases.put(AggregateFunction.STDDEV, "STDEV");
        aggregationAliases.put(AggregateFunction.STDDEV_POP, "STDEVP");
        aggregationAliases.put(AggregateFunction.VARIANCE, "VAR");
        aggregationAliases.put(AggregateFunction.VAR_POP, "VARP");
        return aggregationAliases;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new SybaseSqlGenerationVisitor(this, context);
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
        return "[" + identifier + "]";
    }

    @Override
    public String applyQuoteIfNeeded(final String identifier) {
        return applyQuote(identifier);
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_LOW;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
