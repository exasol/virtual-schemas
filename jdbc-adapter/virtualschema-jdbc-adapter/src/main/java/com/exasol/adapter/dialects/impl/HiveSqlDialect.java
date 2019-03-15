package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.ScalarFunction;

import java.sql.SQLException;
import java.util.EnumMap;
import java.util.Map;

import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

/**
 * Dialect for Hive, using the Cloudera Hive JDBC Driver/Connector (developed by
 * Simba). Only supports Hive 2.1.0 and later because of the order by (nulls
 * first/last option) TODO Finish implementation of this dialect and add as a
 * supported dialect
 */
public class HiveSqlDialect extends AbstractSqlDialect {
    public HiveSqlDialect(final SqlDialectContext context) {
        super(context);
    }

    public static String getPublicName() {
        return "HIVE";
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        builder.addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
              AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT);
        builder.addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IN_CONSTLIST,
              IS_NULL, IS_NOT_NULL);
        builder.addLiteral(NULL, BOOL, DATE, TIMESTAMP, DOUBLE, EXACTNUMERIC, STRING);
        builder.addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
              STDDEV_POP, STDDEV_POP_DISTINCT, STDDEV_SAMP, STDDEV_SAMP_DISTINCT, VAR_POP, VAR_POP_DISTINCT, VAR_SAMP,
              VAR_SAMP_DISTINCT);
        builder.addScalarFunction(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, CEIL, COS, DEGREES, DIV, EXP,
              FLOOR, LN, LOG, MOD, POWER, RADIANS, SIGN, SIN, SQRT, TAN, ASCII, CONCAT, LENGTH, LOWER, LPAD, REPEAT,
              REVERSE, RPAD, SOUNDEX, SPACE, SUBSTR, TRANSLATE, UPPER, ADD_DAYS, ADD_MONTHS, CURRENT_DATE,
              CURRENT_TIMESTAMP, DATE_TRUNC, DAY, DAYS_BETWEEN, MINUTE, MONTH, MONTHS_BETWEEN, SECOND, WEEK, CAST,
              BIT_AND, BIT_OR, BIT_XOR, CURRENT_USER);
        return builder.build();
    }

    /**
     * Quote from user manual The Cloudera JDBC Driver for Apache Hive supports both
     * catalogs and schemas to make it easy for the driver to work with various JDBC
     * applications. Since Hive only organizes tables into schemas/databases, the
     * driver provides a synthetic catalog called “HIVE” under which all of the
     * schemas/databases are organized. The driver also maps the JDBC schema to the
     * Hive schema/database.
     */
    @Override
    public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.UNSUPPORTED;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_LOWER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_LOWER;
    }

    @Override
    public String applyQuote(final String identifier) {
        return "`" + identifier + "`";
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
        // We need schema qualifiers a) if we are in IS_LOCAL mode, i.e. we run
        // statements directly in a subselect without IMPORT FROM JDBC
        // and b) if we don't have the schema in the jdbc connection string (like
        // "jdbc:exa:localhost:5555;schema=native")
        return true;
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+SortBy
        // In Hive 2.1.0 and later, specifying the null sorting order for each of
        // the columns in the "order by" clause is supported. The default null sorting
        // order for ASC order is NULLS FIRST, while the default null sorting order for
        // DESC order is NULLS LAST.
        return NullSorting.NULLS_SORTED_LOW;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new HiveSqlGenerationVisitor(this, context);
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.ADD_DAYS, "DATE_ADD");
        scalarAliases.put(ScalarFunction.DAYS_BETWEEN, "DATEDIFF");
        scalarAliases.put(ScalarFunction.WEEK, "WEEKOFYEAR");
        scalarAliases.put(ScalarFunction.CURRENT_USER, "CURRENT_USER()");
        return scalarAliases;
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcType) throws SQLException {
        return null;
    }
}
