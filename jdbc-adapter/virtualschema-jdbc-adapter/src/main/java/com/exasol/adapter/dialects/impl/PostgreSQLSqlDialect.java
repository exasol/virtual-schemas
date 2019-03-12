package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.DataType;
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

public class PostgreSQLSqlDialect extends AbstractSqlDialect {
    private static final int MAX_POSTGRES_SQL_VARCHAR_SIZE_BASED_ON_EXASOL_LIMIT = 2000000;
    private static final String POSTGRES_IGNORE_UPPERCASE_TABLES = "POSTGRESQL_UPPERCASE_TABLES";
    private static final String NAME = "POSTGRESQL";

    public PostgreSQLSqlDialect(final SqlDialectContext context) {
        super(context);
    }

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
        builder.addLiteral(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING);
        builder.addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
              MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT, STDDEV_SAMP,
              STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT, VAR_SAMP, VAR_SAMP_DISTINCT,
              GROUP_CONCAT);
        builder.addScalarFunction(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COSH, COT,
              DEGREES, DIV, EXP, FLOOR, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SINH,
              SQRT, TAN, TANH, TRUNC, ASCII, BIT_LENGTH, CHR, CONCAT, INSTR, LENGTH, LOWER, LPAD, LTRIM, OCTET_LENGTH,
              REGEXP_REPLACE, REPEAT, REPLACE, REVERSE, RIGHT, RPAD, RTRIM, SUBSTR, TRANSLATE, TRIM, UNICODE,
              UNICODECHR, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS, ADD_SECONDS, ADD_WEEKS, ADD_YEARS,
              SECONDS_BETWEEN, MINUTES_BETWEEN, HOURS_BETWEEN, DAYS_BETWEEN, MONTHS_BETWEEN, YEARS_BETWEEN, MINUTE,
              SECOND, DAY, WEEK, MONTH, YEAR, CURRENT_DATE, CURRENT_TIMESTAMP, DATE_TRUNC, EXTRACT, LOCALTIMESTAMP,
              POSIX_TIME, TO_CHAR, CASE, HASH_MD5);
        return builder.build();
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        DataType colType = null;
        final int jdbcType = jdbcTypeDescription.getJdbcType();
        switch (jdbcType) {
        case Types.OTHER:
            final String columnTypeName = jdbcTypeDescription.getTypeName();
            if (columnTypeName.equals("varbit")) {
                final int n = jdbcTypeDescription.getPrecisionOrSize();
                colType = DataType.createVarChar(n, DataType.ExaCharset.UTF8);
            } else {
                colType = DataType
                      .createVarChar(PostgreSQLSqlDialect.MAX_POSTGRES_SQL_VARCHAR_SIZE_BASED_ON_EXASOL_LIMIT,
                            DataType.ExaCharset.UTF8);
            }
            break;
        case Types.SQLXML:
        case Types.DISTINCT:
            colType = DataType.createVarChar(PostgreSQLSqlDialect.MAX_POSTGRES_SQL_VARCHAR_SIZE_BASED_ON_EXASOL_LIMIT,
                  DataType.ExaCharset.UTF8);
            break;
        default:
            break;
        }

        return colType;
    }

    @Override
    public MappedTable mapTable(final ResultSet tables, final List<String> ignoreErrorList) throws SQLException {
        final String tableName = tables.getString("TABLE_NAME");
        if (getContext().getPostgreSQLIdentifierMapping() == PostgreSQLIdentifierMapping.CONVERT_TO_UPPER &&
              !ignoreErrorList.contains(POSTGRES_IGNORE_UPPERCASE_TABLES) && containsUppercaseCharacter(tableName)) {
            throw new IllegalArgumentException("Table " + tableName + " cannot be used in virtual schema. " +
                  "Set property IGNORE_ERRORS to POSTGRESQL_UPPERCASE_TABLES to enforce schema creation.");
        } else {
            return super.mapTable(tables, ignoreErrorList);
        }
    }

    private boolean containsUppercaseCharacter(final String tableName) {
        return !tableName.equals(tableName.toLowerCase());
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {

        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);

        scalarAliases.put(ScalarFunction.SUBSTR, "SUBSTRING");
        scalarAliases.put(ScalarFunction.HASH_MD5, "MD5");

        return scalarAliases;

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
    public String changeIdentifierCaseIfNeeded(final String identifier) {
        if (getContext().getPostgreSQLIdentifierMapping() != PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE) {
            final boolean isSimplePostgresIdentifier = identifier.matches("^[a-z][0-9a-z_]*");

            if (isSimplePostgresIdentifier) {
                return identifier.toUpperCase();
            }
        }
        return identifier;
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_LOWER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }

    @Override
    public String applyQuote(final String identifier) {
        String postgreSQLIdentifier = identifier;
        if (getContext().getPostgreSQLIdentifierMapping() != PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE) {
            postgreSQLIdentifier = convertIdentifierToLowerCase(postgreSQLIdentifier);
        }
        return "\"" + postgreSQLIdentifier.replace("\"", "\"\"") + "\"";
    }

    private String convertIdentifierToLowerCase(final String identifier) {
        return identifier.toLowerCase();
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
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new PostgresSQLSqlGenerationVisitor(this, context);
    }

}
