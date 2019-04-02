package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.JdbcAdapterProperties;
import com.exasol.adapter.metadata.DataType;

import java.sql.SQLException;
import java.sql.Types;

import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

public class TeradataSqlDialect extends AbstractSqlDialect {
    public final static int maxTeradataVarcharSize = 32000;
    private static final String NAME = "TERADATA";

    public TeradataSqlDialect(final SqlDialectContext context) {
        super();
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
        builder.addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, REGEXP_LIKE, BETWEEN,
              IN_CONSTLIST, IS_NULL, IS_NOT_NULL);
        builder.addLiteral(NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL);
        builder.addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
              MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP);
        builder.addScalarFunction(CEIL, DIV, FLOOR, SIGN, ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2,
              COS, COSH, COT, DEGREES, EXP, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, SIN, SINH, SQRT, TAN, TANH,
              ASCII, CHR, INSTR, LENGTH, LOCATE, LOWER, LPAD, LTRIM, REGEXP_INSTR, REGEXP_REPLACE, REGEXP_SUBSTR,
              REPEAT, REPLACE, REVERSE, RPAD, RTRIM, SOUNDEX, SUBSTR, TRANSLATE, TRIM, UPPER, ADD_DAYS, ADD_HOURS,
              ADD_MINUTES, ADD_MONTHS, ADD_SECONDS, ADD_WEEKS, ADD_YEARS, CURRENT_DATE, CURRENT_TIMESTAMP, NULLIFZERO,
              ZEROIFNULL, TRUNC, ROUND);
        return builder.build();
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        DataType colType = null;
        final int jdbcType = jdbcTypeDescription.getJdbcType();
        switch (jdbcType) {
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
        case Types.OTHER: // Teradata JDBC uses OTHER for several data types GEOMETRY, INTERVAL etc...
            final String columnTypeName = jdbcTypeDescription.getTypeName();

            if (columnTypeName.equals("GEOMETRY")) {
                colType = DataType.createVarChar(jdbcTypeDescription.getPrecisionOrSize(), DataType.ExaCharset.UTF8);
            } else if (columnTypeName.startsWith("INTERVAL")) {
                colType = DataType.createVarChar(30, DataType.ExaCharset.UTF8); // TODO verify that varchar 30 is
                // sufficient in all cases
            } else if (columnTypeName.startsWith("PERIOD")) {
                colType = DataType.createVarChar(100, DataType.ExaCharset.UTF8);
            } else {
                colType = DataType.createVarChar(TeradataSqlDialect.maxTeradataVarcharSize, DataType.ExaCharset.UTF8);
            }
            break;
        case Types.SQLXML:
            colType = DataType.createVarChar(TeradataSqlDialect.maxTeradataVarcharSize, DataType.ExaCharset.UTF8);
            break;
        case Types.CLOB:
            colType = DataType.createVarChar(TeradataSqlDialect.maxTeradataVarcharSize, DataType.ExaCharset.UTF8);
            break;
        case Types.BLOB:
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
    public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.UNSUPPORTED;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new TeradataSqlGenerationVisitor(this, context);
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
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String applyQuoteIfNeeded(final String identifier) {
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
    public void handleException(final SQLException exception,
          final JdbcAdapterProperties.ExceptionHandlingMode exceptionMode) throws SQLException {
        if (exceptionMode == JdbcAdapterProperties.ExceptionHandlingMode.IGNORE_INVALID_VIEWS) {
            if (exception.getMessage().contains("Teradata Database") && exception.getMessage().contains("Error 3807")) {
                return;
            }
        }
        throw exception;
    }
}
