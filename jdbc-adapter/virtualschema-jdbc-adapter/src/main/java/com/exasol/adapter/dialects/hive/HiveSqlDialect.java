package com.exasol.adapter.dialects.hive;

import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.Connection;
import java.util.EnumMap;
import java.util.Map;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * Dialect for Hive, using the Cloudera Hive JDBC Driver/Connector (developed by Simba). Only supports Hive 2.1.0 and
 * later because of the order by (nulls first/last option) TODO Finish implementation of this dialect and add as a
 * supported dialect
 */
public class HiveSqlDialect extends AbstractSqlDialect {
    private static final String NAME = "HIVE";

    public HiveSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    public static String getPublicName() {
        return NAME;
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
     * Quote from user manual The Cloudera JDBC Driver for Apache Hive supports both catalogs and schemas to make it
     * easy for the driver to work with various JDBC applications. Since Hive only organizes tables into
     * schemas/databases, the driver provides a synthetic catalog called “HIVE” under which all of the schemas/databases
     * are organized. The driver also maps the JDBC schema to the Hive schema/database.
     */
    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.SINGLE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
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
    public void validateProperties() throws PropertyValidationException {
        super.validateDialectName(getPublicName());
        super.validateProperties();
    }
}