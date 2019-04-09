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

public class RedshiftSqlDialect extends AbstractSqlDialect {
    public RedshiftSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    private static final String NAME = "REDSHIFT";

    public static String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        builder.addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING,
                ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET);
        builder.addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN, REGEXP_LIKE,
                IN_CONSTLIST, IS_NULL, IS_NOT_NULL);
        builder.addLiteral(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL);
        builder.addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
                MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT, STDDEV_SAMP,
                STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT, VAR_SAMP,
                VAR_SAMP_DISTINCT, GROUP_CONCAT);
        builder.addScalarFunction(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COT, DEGREES,
                DIV, EXP, FLOOR, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, ROUND, SIGN, SIN, SINH, SQRT, TAN, TANH,
                TRUNC, ASCII, CHR, CONCAT, LOCATE, INSTR, LENGTH, LOWER, LPAD, LTRIM, REGEXP_INSTR, REGEXP_REPLACE,
                REGEXP_SUBSTR, REPEAT, REPLACE, REVERSE, RIGHT, RPAD, RTRIM, SUBSTR, TRANSLATE, TRIM, UPPER, BIT_AND,
                BIT_OR, ADD_MONTHS, MONTHS_BETWEEN, CONVERT_TZ, SYSDATE, YEAR, CURRENT_DATE, CURRENT_TIMESTAMP, EXTRACT,
                CAST, TO_NUMBER, TO_TIMESTAMP, TO_DATE, HASH_SHA1, HASH_MD5, CURRENT_SCHEMA, CURRENT_USER);
        return builder.build();
    }

    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        DataType colType = null;
        final int jdbcType = jdbcTypeDescription.getJdbcType();
        switch (jdbcType) {
        case Types.NUMERIC:
            final int decimalPrec = jdbcTypeDescription.getPrecisionOrSize();
            final int decimalScale = jdbcTypeDescription.getDecimalScale();
            if (decimalPrec <= DataType.MAX_EXASOL_DECIMAL_PRECISION) {
                colType = DataType.createDecimal(decimalPrec, decimalScale);
            } else {
                colType = DataType.createDouble();
            }
            break;
        }
        return colType;
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.YEAR, "DATE_PART_YEAR");
        scalarAliases.put(ScalarFunction.CONVERT_TZ, "CONVERT_TIMEZONE");
        scalarAliases.put(ScalarFunction.HASH_MD5, "MD5");
        scalarAliases.put(ScalarFunction.HASH_SHA1, "FUNC_SHA1");
        scalarAliases.put(ScalarFunction.SUBSTR, "SUBSTRING");
        return scalarAliases;
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        return new EnumMap<>(AggregateFunction.class);
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
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_UPPER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_UPPER;
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
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new RedshiftSqlGenerationVisitor(this, context);
    }
}
