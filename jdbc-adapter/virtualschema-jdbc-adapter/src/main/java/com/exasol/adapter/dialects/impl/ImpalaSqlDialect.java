package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.AbstractSqlDialect;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;

/**
 * Dialect for Impala, using the Cloudera Impala JDBC Driver/Connector (developed by Simba).
 * 
 * See http://www.cloudera.com/documentation/enterprise/latest/topics/impala_langref.html
 */
public class ImpalaSqlDialect extends AbstractSqlDialect {

    public ImpalaSqlDialect(SqlDialectContext context) {
        super(context);
    }

    public static final String NAME = "IMPALA";

    public String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        // Main capabilities
        Capabilities cap = new Capabilities();
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

        // Literals
        cap.supportLiteral(LiteralCapability.STRING);
        cap.supportLiteral(LiteralCapability.BOOL);
        cap.supportLiteral(LiteralCapability.EXACTNUMERIC);
        cap.supportLiteral(LiteralCapability.DOUBLE);
        cap.supportLiteral(LiteralCapability.NULL);
        // TODO Implement timestamp literal

        // Predicates
        cap.supportPredicate(PredicateCapability.AND);
        cap.supportPredicate(PredicateCapability.OR);
        cap.supportPredicate(PredicateCapability.NOT);
        cap.supportPredicate(PredicateCapability.EQUAL);
        cap.supportPredicate(PredicateCapability.NOTEQUAL);
        cap.supportPredicate(PredicateCapability.LESS);
        cap.supportPredicate(PredicateCapability.LESSEQUAL);
        cap.supportPredicate(PredicateCapability.LIKE);
        // LIKE_ESCAPE is not supported
        cap.supportPredicate(PredicateCapability.REGEXP_LIKE);
        cap.supportPredicate(PredicateCapability.BETWEEN);
        cap.supportPredicate(PredicateCapability.IN_CONSTLIST);
        cap.supportPredicate(PredicateCapability.IS_NULL);
        cap.supportPredicate(PredicateCapability.IS_NOT_NULL);

        // Aggregate Functions
        // Unsupported by EXASOL: APPX_MEDIAN (approximate median)
        // With Alias: NDV (approximate count distinct)
        cap.supportAggregateFunction(AggregateFunctionCapability.AVG);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_STAR);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_DISTINCT);
        // GROUP_CONCAT with DISTINCT not supported
        cap.supportAggregateFunction(AggregateFunctionCapability.GROUP_CONCAT);
        cap.supportAggregateFunction(AggregateFunctionCapability.GROUP_CONCAT_SEPARATOR);
        cap.supportAggregateFunction(AggregateFunctionCapability.MAX);
        cap.supportAggregateFunction(AggregateFunctionCapability.MIN);
        cap.supportAggregateFunction(AggregateFunctionCapability.SUM);
        cap.supportAggregateFunction(AggregateFunctionCapability.SUM_DISTINCT);

        // TODO Scalar Functions
        
        return cap;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.UNSUPPORTED;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    /**
     * Note from Impala documentation: Impala identifiers are always
     * case-insensitive. That is, tables named t1 and T1 always refer to the
     * same table, regardless of quote characters. Internally, Impala always
     * folds all specified table and column names to lowercase. This is why the
     * column headers in query output are always displayed in lowercase.
     */
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
        // If identifier contains double quotation marks ", it needs to be espaced by another double quotation mark. E.g. "a""b" is the identifier a"b in the db.
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
    public SqlGenerationVisitor getSqlGenerationVisitor(SqlGenerationContext context) {
        return new ImpalaSqlGenerationVisitor(this, context);
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        // In Impala 1.2.1 and higher, all NULL values come at the end of the result set for ORDER BY ... ASC queries,
        // and at the beginning of the result set for ORDER BY ... DESC queries.
        // In effect, NULL is considered greater than all other values for sorting purposes.
        // The original Impala behavior always put NULL values at the end, even for ORDER BY ... DESC queries.
        // The new behavior in Impala 1.2.1 makes Impala more compatible with other popular database systems.
        // In Impala 1.2.1 and higher, you can override or specify the sorting behavior for NULL by adding the clause
        // NULLS FIRST or NULLS LAST at the end of the ORDER BY clause.
        return NullSorting.NULLS_SORTED_HIGH;
    }

    @Override
    public String getStringLiteral(String value) {
        // Don't forget to escape single quote
        return "'" + value.replace("'", "''") + "'";
    }

}
