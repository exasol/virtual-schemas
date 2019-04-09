package com.exasol.adapter.dialects.impl;

import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.*;
import java.util.EnumMap;
import java.util.Map;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

public class SqlServerSqlDialect extends AbstractSqlDialect {
    // Tested SQL Server versions: SQL Server 2014
    // Tested JDBC drivers: jtds-1.3.1 (https://sourceforge.net/projects/jtds/)
    public final static int maxSqlServerVarcharSize = 8000;
    public final static int maxSqlServerNVarcharSize = 4000;
    public final static int maxSqlServerClob = 2000000;
    private static final String NAME = "SQLSERVER";

    public SqlServerSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    public static String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        builder.addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING,
                ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT);
        builder.addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN, REGEXP_LIKE,
                IN_CONSTLIST, IS_NULL, IS_NOT_NULL);
        builder.addLiteral(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL);
        builder.addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
                MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT, VARIANCE,
                VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT);
        builder.addScalarFunction(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COT, DEGREES,
                EXP, FLOOR, LOG, MOD, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SQRT, TAN, TRUNC, ASCII, CHR, CONCAT,
                INSTR, LENGTH, LOCATE, LOWER, LPAD, LTRIM, REPEAT, REPLACE, REVERSE, RIGHT, RPAD, RTRIM, SOUNDEX, SPACE,
                SUBSTR, TRIM, UNICODE, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS, ADD_SECONDS, ADD_WEEKS,
                ADD_YEARS, SECONDS_BETWEEN, MINUTES_BETWEEN, HOURS_BETWEEN, DAYS_BETWEEN, MONTHS_BETWEEN, YEARS_BETWEEN,
                DAY, MONTH, YEAR, SYSDATE, SYSTIMESTAMP, CURRENT_DATE, CURRENT_TIMESTAMP, ST_X, ST_Y, ST_ENDPOINT,
                ST_ISCLOSED, ST_ISRING, ST_LENGTH, ST_NUMPOINTS, ST_POINTN, ST_STARTPOINT, ST_AREA, ST_EXTERIORRING,
                ST_INTERIORRINGN, ST_NUMINTERIORRINGS, ST_GEOMETRYN, ST_NUMGEOMETRIES, ST_BOUNDARY, ST_BUFFER,
                ST_CENTROID, ST_CONTAINS, ST_CONVEXHULL, ST_CROSSES, ST_DIFFERENCE, ST_DIMENSION, ST_DISJOINT,
                ST_DISTANCE, ST_ENVELOPE, ST_EQUALS, ST_GEOMETRYTYPE, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY,
                ST_ISSIMPLE, ST_OVERLAPS, ST_SYMDIFFERENCE, ST_TOUCHES, ST_UNION, ST_WITHIN, BIT_AND, BIT_NOT, BIT_OR,
                BIT_XOR, CASE, HASH_MD5, HASH_SHA, HASH_SHA1, NULLIFZERO, ZEROIFNULL);
        return builder.build();
    }

    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        DataType colType = null;
        final int jdbcType = jdbcTypeDescription.getJdbcType();
        final String columnTypeName = jdbcTypeDescription.getTypeName();

        switch (jdbcType) {
        case Types.VARCHAR:
            if (columnTypeName.equalsIgnoreCase("date")) {
                colType = DataType.createDate();
            } else if (columnTypeName.equalsIgnoreCase("datetime2")) {
                colType = DataType.createTimestamp(false);
            }
            break;
        case Types.TIME:
            colType = DataType.createVarChar(21, DataType.ExaCharset.UTF8);
            break;
        case 2013: // Types.TIME_WITH_TIMEZONE is Java 1.8 specific
            colType = DataType.createVarChar(21, DataType.ExaCharset.UTF8);
            break;
        case Types.NUMERIC:
            final int decimalPrec = jdbcTypeDescription.getPrecisionOrSize();
            final int decimalScale = jdbcTypeDescription.getDecimalScale();

            if (decimalPrec <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
                colType = DataType.createDecimal(decimalPrec, decimalScale);
            } else {
                colType = DataType.createDouble();
            }
            break;

        case Types.OTHER:
            // TODO
            colType = DataType.createVarChar(SqlServerSqlDialect.maxSqlServerVarcharSize, DataType.ExaCharset.UTF8);
            break;
        case Types.SQLXML:
            colType = DataType.createVarChar(SqlServerSqlDialect.maxSqlServerVarcharSize, DataType.ExaCharset.UTF8);
            break;
        case Types.CLOB: // xml type in SQL Server
            colType = DataType.createVarChar(SqlServerSqlDialect.maxSqlServerClob, DataType.ExaCharset.UTF8);
            break;
        case Types.BLOB:
            if (columnTypeName.equalsIgnoreCase("hierarchyid")) {
                colType = DataType.createVarChar(4000, DataType.ExaCharset.UTF8);
            }
            if (columnTypeName.equalsIgnoreCase("geometry")) {
                colType = DataType.createVarChar(SqlServerSqlDialect.maxSqlServerVarcharSize, DataType.ExaCharset.UTF8);
            } else {
                colType = DataType.createVarChar(100, DataType.ExaCharset.UTF8);
            }
            break;
        case Types.VARBINARY:
        case Types.BINARY:
            colType = DataType.createVarChar(100, DataType.ExaCharset.UTF8);
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
        return new SqlServerSqlGenerationVisitor(this, context);
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
        return NullSorting.NULLS_SORTED_AT_START;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
