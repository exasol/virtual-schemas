package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.AbstractSqlDialect;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.sql.ScalarFunction;

import java.util.EnumMap;
import java.util.Map;

/**
 * Dialect for Hive, using the Cloudera Hive JDBC Driver/Connector (developed by Simba).
 * Only supports Hive 2.1.0 and later because of the order by (nulls first/last option)
 * TODO Finish implementation of this dialect and add as a supported dialect
 */
public class HiveSqlDialect extends AbstractSqlDialect {

    public HiveSqlDialect(SqlDialectContext context) {
        super(context);
    }

    public static final String NAME = "HIVE";

    public String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        Capabilities cap = new Capabilities();
        cap.supportMainCapability(MainCapability.SELECTLIST_PROJECTION);
        cap.supportMainCapability(MainCapability.SELECTLIST_EXPRESSIONS);
        cap.supportMainCapability(MainCapability.FILTER_EXPRESSIONS);
        cap.supportMainCapability(MainCapability.AGGREGATE_SINGLE_GROUP);
        cap.supportMainCapability(MainCapability.AGGREGATE_GROUP_BY_COLUMN);
        cap.supportMainCapability(MainCapability.AGGREGATE_HAVING);
        cap.supportMainCapability(MainCapability.ORDER_BY_COLUMN);
        cap.supportMainCapability(MainCapability.ORDER_BY_EXPRESSION);
        cap.supportMainCapability(MainCapability.LIMIT);

        cap.supportLiteral(LiteralCapability.NULL);
        cap.supportLiteral(LiteralCapability.BOOL);
        cap.supportLiteral(LiteralCapability.DATE);
        cap.supportLiteral(LiteralCapability.TIMESTAMP);
        cap.supportLiteral(LiteralCapability.DOUBLE);
        cap.supportLiteral(LiteralCapability.EXACTNUMERIC);
        cap.supportLiteral(LiteralCapability.STRING);

        cap.supportPredicate(PredicateCapability.AND);
        cap.supportPredicate(PredicateCapability.OR);
        cap.supportPredicate(PredicateCapability.NOT);
        cap.supportPredicate(PredicateCapability.EQUAL);
        cap.supportPredicate(PredicateCapability.NOTEQUAL);
        cap.supportPredicate(PredicateCapability.LESS);
        cap.supportPredicate(PredicateCapability.LESSEQUAL);
        cap.supportPredicate(PredicateCapability.LIKE);
        cap.supportPredicate(PredicateCapability.REGEXP_LIKE);
        cap.supportPredicate(PredicateCapability.BETWEEN);
        cap.supportPredicate(PredicateCapability.IN_CONSTLIST);
        cap.supportPredicate(PredicateCapability.IS_NULL);
        cap.supportPredicate(PredicateCapability.IS_NOT_NULL);

        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_STAR);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.SUM);
        cap.supportAggregateFunction(AggregateFunctionCapability.SUM_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.MIN);
        cap.supportAggregateFunction(AggregateFunctionCapability.MAX);
        cap.supportAggregateFunction(AggregateFunctionCapability.AVG);
        cap.supportAggregateFunction(AggregateFunctionCapability.AVG_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_POP);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_POP_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_SAMP);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_SAMP_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_SAMP);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_SAMP_DISTINCT);

        cap.supportScalarFunction(ScalarFunctionCapability.ADD);
        cap.supportScalarFunction(ScalarFunctionCapability.SUB);
        cap.supportScalarFunction(ScalarFunctionCapability.MULT);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOAT_DIV);
        cap.supportScalarFunction(ScalarFunctionCapability.NEG);
        cap.supportScalarFunction(ScalarFunctionCapability.ABS);
        cap.supportScalarFunction(ScalarFunctionCapability.ACOS);
        cap.supportScalarFunction(ScalarFunctionCapability.ASIN);
        cap.supportScalarFunction(ScalarFunctionCapability.ATAN);
        cap.supportScalarFunction(ScalarFunctionCapability.CEIL);
        cap.supportScalarFunction(ScalarFunctionCapability.COS);
        cap.supportScalarFunction(ScalarFunctionCapability.DEGREES);
        cap.supportScalarFunction(ScalarFunctionCapability.DIV);
        cap.supportScalarFunction(ScalarFunctionCapability.EXP);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOOR);
        cap.supportScalarFunction(ScalarFunctionCapability.LN);
        cap.supportScalarFunction(ScalarFunctionCapability.LOG);
        cap.supportScalarFunction(ScalarFunctionCapability.MOD);
        cap.supportScalarFunction(ScalarFunctionCapability.POWER);
        cap.supportScalarFunction(ScalarFunctionCapability.RADIANS);
        cap.supportScalarFunction(ScalarFunctionCapability.SIGN);
        cap.supportScalarFunction(ScalarFunctionCapability.SIN);
        cap.supportScalarFunction(ScalarFunctionCapability.SQRT);
        cap.supportScalarFunction(ScalarFunctionCapability.TAN);

        cap.supportScalarFunction(ScalarFunctionCapability.ASCII);
        cap.supportScalarFunction(ScalarFunctionCapability.CONCAT);
        cap.supportScalarFunction(ScalarFunctionCapability.LENGTH);
        cap.supportScalarFunction(ScalarFunctionCapability.LOWER);
        cap.supportScalarFunction(ScalarFunctionCapability.LPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.REPEAT);
        cap.supportScalarFunction(ScalarFunctionCapability.REVERSE);
        cap.supportScalarFunction(ScalarFunctionCapability.RPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.SOUNDEX);
        cap.supportScalarFunction(ScalarFunctionCapability.SPACE);
        cap.supportScalarFunction(ScalarFunctionCapability.SUBSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.TRANSLATE);
        cap.supportScalarFunction(ScalarFunctionCapability.UPPER);

        cap.supportScalarFunction(ScalarFunctionCapability.ADD_DAYS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MONTHS);
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_DATE);
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_TIMESTAMP);
        cap.supportScalarFunction(ScalarFunctionCapability.DATE_TRUNC);
        cap.supportScalarFunction(ScalarFunctionCapability.DAY);
        cap.supportScalarFunction(ScalarFunctionCapability.DAYS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.MINUTE);
        cap.supportScalarFunction(ScalarFunctionCapability.MONTH);
        cap.supportScalarFunction(ScalarFunctionCapability.MONTHS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.SECOND);
        cap.supportScalarFunction(ScalarFunctionCapability.WEEK);

        /*hive doesn't support geospatial functions*/

        cap.supportScalarFunction(ScalarFunctionCapability.CAST);

        cap.supportScalarFunction(ScalarFunctionCapability.BIT_AND);
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_OR);
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_XOR);

        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_USER);

        return cap;
    }

    /**
     * Quote from user manual The Cloudera JDBC Driver for Apache Hive supports both catalogs and schemas to make it easy for
     * the driver to work with various JDBC applications. Since Hive only organizes tables into
     * schemas/databases, the driver provides a synthetic catalog called “HIVE” under which all of the
     * schemas/databases are organized. The driver also maps the JDBC schema to the Hive
     * schema/database.
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
    public String applyQuote(String identifier) {
        // If identifier contains double quotation marks ", it needs to be escaped by another double quotation mark. E.g. "a""b" is the identifier a"b in the db.
        return "`" + identifier + "`";
    }

    @Override
    public String applyQuoteIfNeeded(String identifier) {
        // We need to apply quotes only in case of reserved keywords. Since we don't know these (could look up in JDBC Metadata...) we always quote.
        return applyQuote(identifier);
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(SqlGenerationContext context) {
        // We need schema qualifiers a) if we are in IS_LOCAL mode, i.e. we run statements directly in a subselect without IMPORT FROM JDBC
        // and b) if we don't have the schema in the jdbc connection string (like "jdbc:exa:localhost:5555;schema=native")
        return true;
        // return context.isLocal();
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
    public String getStringLiteral(String value) {
        // Don't forget to escape single quote
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(SqlGenerationContext context) {
        return new HiveSqlGenerationVisitor(this, context);
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {

        Map<ScalarFunction,String> scalarAliases = new EnumMap<>(ScalarFunction.class);

        scalarAliases.put(ScalarFunction.ADD_DAYS, "DATE_ADD");
        scalarAliases.put(ScalarFunction.DAYS_BETWEEN, "DATEDIFF");
        scalarAliases.put(ScalarFunction.WEEK, "WEEKOFYEAR");
        scalarAliases.put(ScalarFunction.CURRENT_USER, "CURRENT_USER()");

        return scalarAliases;

    }

}
