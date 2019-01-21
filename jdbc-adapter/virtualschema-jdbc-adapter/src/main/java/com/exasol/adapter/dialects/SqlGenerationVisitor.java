package com.exasol.adapter.dialects;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;
import com.exasol.adapter.sql.SqlColumn;
import com.exasol.adapter.sql.SqlFunctionAggregate;
import com.exasol.adapter.sql.SqlFunctionAggregateGroupConcat;
import com.exasol.adapter.sql.SqlFunctionScalar;
import com.exasol.adapter.sql.SqlFunctionScalarCase;
import com.exasol.adapter.sql.SqlFunctionScalarCast;
import com.exasol.adapter.sql.SqlFunctionScalarExtract;
import com.exasol.adapter.sql.SqlGroupBy;
import com.exasol.adapter.sql.SqlJoin;
import com.exasol.adapter.sql.SqlLimit;
import com.exasol.adapter.sql.SqlLiteralBool;
import com.exasol.adapter.sql.SqlLiteralDate;
import com.exasol.adapter.sql.SqlLiteralDouble;
import com.exasol.adapter.sql.SqlLiteralExactnumeric;
import com.exasol.adapter.sql.SqlLiteralInterval;
import com.exasol.adapter.sql.SqlLiteralNull;
import com.exasol.adapter.sql.SqlLiteralString;
import com.exasol.adapter.sql.SqlLiteralTimestamp;
import com.exasol.adapter.sql.SqlLiteralTimestampUtc;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlNodeVisitor;
import com.exasol.adapter.sql.SqlOrderBy;
import com.exasol.adapter.sql.SqlPredicateAnd;
import com.exasol.adapter.sql.SqlPredicateBetween;
import com.exasol.adapter.sql.SqlPredicateEqual;
import com.exasol.adapter.sql.SqlPredicateInConstList;
import com.exasol.adapter.sql.SqlPredicateIsNotNull;
import com.exasol.adapter.sql.SqlPredicateIsNull;
import com.exasol.adapter.sql.SqlPredicateLess;
import com.exasol.adapter.sql.SqlPredicateLessEqual;
import com.exasol.adapter.sql.SqlPredicateLike;
import com.exasol.adapter.sql.SqlPredicateLikeRegexp;
import com.exasol.adapter.sql.SqlPredicateNot;
import com.exasol.adapter.sql.SqlPredicateNotEqual;
import com.exasol.adapter.sql.SqlPredicateOr;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.adapter.sql.SqlTable;
import com.google.common.base.Joiner;

/**
 * This class has the logic to generate SQL queries based on a graph of
 * {@link SqlNode} elements. It uses the visitor pattern. This class interacts
 * with the dialects in some situations, e.g. to find out how to handle quoting,
 * case-sensitivity.
 *
 * <p>
 * If this class is not sufficiently customizable for your use case, you can
 * extend this class and override the required methods. You also have to return
 * your custom visitor class then in the method
 * {@link SqlDialect#getSqlGenerationVisitor(SqlGenerationContext)}. See
 * {@link com.exasol.adapter.dialects.impl.OracleSqlGenerationVisitor} for an
 * example.
 * </p>
 *
 * Note on operator associativity and parenthesis generation: Currently we use
 * parenthesis almost always. Without parenthesis, two SqlNode graphs with
 * different semantic lead to "select 1 = 1 - 1 + 1". Also "SELECT NOT NOT TRUE"
 * needs to be written as "SELECT NOT (NOT TRUE)" to work at all, whereas SELECT
 * NOT TRUE works fine without parentheses. Currently we make inflationary use
 * of parenthesis to to enforce the right semantic, but hopefully there is a
 * better way.
 */
public class SqlGenerationVisitor implements SqlNodeVisitor<String> {

    private final SqlDialect dialect;
    private final SqlGenerationContext context;

    public SqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        this.dialect = dialect;
        this.context = context;

        checkDialectAliases();
    }

    protected void checkDialectAliases() {
        // Check if dialect provided invalid aliases, which would never be applied.
        for (final ScalarFunction function : this.dialect.getScalarFunctionAliases().keySet()) {
            if (!function.isSimple()) {
                throw new RuntimeException("The dialect " + SqlDialect.getPublicName()
                        + " provided an alias for the non-simple scalar function " + function.name()
                        + ". This alias will never be considered.");
            }
        }
        for (final AggregateFunction function : this.dialect.getAggregateFunctionAliases().keySet()) {
            if (!function.isSimple()) {
                throw new RuntimeException("The dialect " + SqlDialect.getPublicName()
                        + " provided an alias for the non-simple aggregate function " + function.name()
                        + ". This alias will never be considered.");
            }
        }
    }

    @Override
    public String visit(final SqlStatementSelect select) throws AdapterException {
        final StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append(select.getSelectList().accept(this));
        sql.append(" FROM ");
        sql.append(select.getFromClause().accept(this));
        if (select.hasFilter()) {
            sql.append(" WHERE ");
            sql.append(select.getWhereClause().accept(this));
        }
        if (select.hasGroupBy()) {
            sql.append(" GROUP BY ");
            sql.append(select.getGroupBy().accept(this));
        }
        if (select.hasHaving()) {
            sql.append(" HAVING ");
            sql.append(select.getHaving().accept(this));
        }
        if (select.hasOrderBy()) {
            sql.append(" ");
            sql.append(select.getOrderBy().accept(this));
        }
        if (select.hasLimit()) {
            sql.append(" ");
            sql.append(select.getLimit().accept(this));
        }
        return sql.toString();
    }

    @Override
    public String visit(final SqlSelectList selectList) throws AdapterException {
        final List<String> selectElement = new ArrayList<>();
        if (selectList.isRequestAnyColumn()) {
            // The system requested any column
            selectElement.add("true");
        } else if (selectList.isSelectStar()) {
            selectElement.add("*");
        } else {
            for (final SqlNode node : selectList.getExpressions()) {
                selectElement.add(node.accept(this));
            }
        }
        return Joiner.on(", ").join(selectElement);
    }

    @Override
    public String visit(final SqlColumn column) throws AdapterException {
        String tablePrefix = "";
        if (this.context.hasMoreThanOneTable()) {
            if (column.hasTableAlias()) {
                tablePrefix = this.dialect.applyQuoteIfNeeded(column.getTableAlias())
                        + this.dialect.getTableCatalogAndSchemaSeparator();
            } else {
                tablePrefix = this.dialect.applyQuoteIfNeeded(column.getTableName())
                        + this.dialect.getTableCatalogAndSchemaSeparator();
            }
        }
        return tablePrefix + this.dialect.applyQuoteIfNeeded(column.getName());
    }

    @Override
    public String visit(final SqlTable table) {
        String schemaPrefix = "";
        if (this.dialect.requiresCatalogQualifiedTableNames(this.context) && this.context.getCatalogName() != null
                && !this.context.getCatalogName().isEmpty()) {
            schemaPrefix = this.dialect.applyQuoteIfNeeded(this.context.getCatalogName())
                    + this.dialect.getTableCatalogAndSchemaSeparator();
        }
        if (this.dialect.requiresSchemaQualifiedTableNames(this.context) && this.context.getSchemaName() != null
                && !this.context.getSchemaName().isEmpty()) {
            schemaPrefix += this.dialect.applyQuoteIfNeeded(this.context.getSchemaName())
                    + this.dialect.getTableCatalogAndSchemaSeparator();
        }
        if (table.hasAlias()) {
            return schemaPrefix + this.dialect.applyQuoteIfNeeded(table.getName())
                    + " " + this.dialect.applyQuoteIfNeeded(table.getAlias());
        } else {
            return schemaPrefix + this.dialect.applyQuoteIfNeeded(table.getName());
        }
    }

    @Override
    public String visit(final SqlJoin join) throws AdapterException {
        return join.getLeft().accept(this) + " " + join.getJoinType().name().replace('_', ' ')
                 + " JOIN " + join.getRight().accept(this)
                 + " ON " + join.getCondition().accept(this);
    }

    @Override
    public String visit(final SqlGroupBy groupBy) throws AdapterException {
        if (groupBy.getExpressions() == null || groupBy.getExpressions().isEmpty()) {
            throw new RuntimeException("Unexpected internal state (empty group by)");
        }
        final List<String> selectElement = new ArrayList<>();
        for (final SqlNode node : groupBy.getExpressions()) {
            selectElement.add(node.accept(this));
        }
        return Joiner.on(", ").join(selectElement);
    }

    @Override
    public String visit(final SqlFunctionAggregate function) throws AdapterException {
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        if (function.getFunctionName().equalsIgnoreCase("count") && argumentsSql.size() == 0) {
            argumentsSql.add("*");
        }
        String distinctSql = "";
        if (function.hasDistinct()) {
            distinctSql = "DISTINCT ";
        }
        String functionNameInSourceSystem = function.getFunctionName();
        if (this.dialect.getAggregateFunctionAliases().containsKey(function.getFunction())) {
            functionNameInSourceSystem = this.dialect.getAggregateFunctionAliases().get(function.getFunction());
        }
        return functionNameInSourceSystem + "(" + distinctSql + Joiner.on(", ").join(argumentsSql) + ")";
    }

    @Override
    public String visit(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        builder.append(function.getFunctionName());
        builder.append("(");
        if (function.hasDistinct()) {
            builder.append("DISTINCT ");
        }
        assert (function.getArguments().size() == 1 && function.getArguments().get(0) != null);
        builder.append(function.getArguments().get(0).accept(this));
        if (function.hasOrderBy()) {
            builder.append(" ");
            final String orderByString = function.getOrderBy().accept(this);
            builder.append(orderByString);
        }
        if (function.getSeparator() != null) {
            builder.append(" SEPARATOR ");
            builder.append("'");
            builder.append(function.getSeparator());
            builder.append("'");
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        String functionNameInSourceSystem = function.getFunctionName();
        if (this.dialect.getScalarFunctionAliases().containsKey(function.getFunction())) {
            // Take alias if one is defined - will overwrite the infix
            functionNameInSourceSystem = this.dialect.getScalarFunctionAliases().get(function.getFunction());
        } else {
            if (this.dialect.getBinaryInfixFunctionAliases().containsKey(function.getFunction())) {
                assert (argumentsSql.size() == 2);
                String realFunctionName = function.getFunctionName();
                if (this.dialect.getBinaryInfixFunctionAliases().containsKey(function.getFunction())) {
                    realFunctionName = this.dialect.getBinaryInfixFunctionAliases().get(function.getFunction());
                }
                return "(" + argumentsSql.get(0) + " " + realFunctionName + " " + argumentsSql.get(1) + ")";
            } else if (this.dialect.getPrefixFunctionAliases().containsKey(function.getFunction())) {
                assert (argumentsSql.size() == 1);
                String realFunctionName = function.getFunctionName();
                if (this.dialect.getPrefixFunctionAliases().containsKey(function.getFunction())) {
                    realFunctionName = this.dialect.getPrefixFunctionAliases().get(function.getFunction());
                }
                return "(" + realFunctionName + argumentsSql.get(0) + ")";
            }
        }
        if (argumentsSql.size() == 0 && this.dialect.omitParentheses(function.getFunction())) {
            return functionNameInSourceSystem;
        } else {
            return functionNameInSourceSystem + "(" + Joiner.on(", ").join(argumentsSql) + ")";
        }
    }

    @Override
    public String visit(final SqlFunctionScalarCase function) throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        builder.append("CASE");
        if (function.getBasis() != null) {
            builder.append(" ");
            builder.append(function.getBasis().accept(this));
        }
        for (int i = 0; i < function.getArguments().size(); i++) {
            final SqlNode node = function.getArguments().get(i);
            final SqlNode result = function.getResults().get(i);
            builder.append(" WHEN ");
            builder.append(node.accept(this));
            builder.append(" THEN ");
            builder.append(result.accept(this));
        }
        if (function.getResults().size() > function.getArguments().size()) {
            builder.append(" ELSE ");
            builder.append(function.getResults().get(function.getResults().size() - 1).accept(this));
        }
        builder.append(" END");
        return builder.toString();
    }

    @Override
    public String visit(final SqlFunctionScalarCast function) throws AdapterException {

        final StringBuilder builder = new StringBuilder();
        builder.append("CAST");
        builder.append("(");
        assert (function.getArguments().size() == 1 && function.getArguments().get(0) != null);
        builder.append(function.getArguments().get(0).accept(this));
        builder.append(" AS ");
        builder.append(function.getDataType());
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String visit(final SqlFunctionScalarExtract function) throws AdapterException {
        assert (function.getArguments().size() == 1 && function.getArguments().get(0) != null);
        final String expression = function.getArguments().get(0).accept(this);
        return function.getFunctionName() + "(" + function.getToExtract() + " FROM " + expression + ")";
    }

    @Override
    public String visit(final SqlLimit limit) {
        String offsetSql = "";
        if (limit.getOffset() != 0) {
            offsetSql = " OFFSET " + limit.getOffset();
        }
        return "LIMIT " + limit.getLimit() + offsetSql;
    }

    @Override
    public String visit(final SqlLiteralBool literal) {
        if (literal.getValue()) {
            return "true";
        } else {
            return "false";
        }
    }

    @Override
    public String visit(final SqlLiteralDate literal) {
        return "DATE '" + literal.getValue() + "'"; // This gets always executed
                                                    // as
                                                    // TO_DATE('2015-02-01','YYYY-MM-DD')
    }

    @Override
    public String visit(final SqlLiteralDouble literal) {
        return Double.toString(literal.getValue());
    }

    @Override
    public String visit(final SqlLiteralExactnumeric literal) {
        return literal.getValue().toString();
    }

    @Override
    public String visit(final SqlLiteralNull literal) {
        return "NULL";
    }

    @Override
    public String visit(final SqlLiteralString literal) {
        return this.dialect.getStringLiteral(literal.getValue());
    }

    @Override
    public String visit(final SqlLiteralTimestamp literal) {
        // TODO Allow dialect to modify behavior
        return "TIMESTAMP '" + literal.getValue().toString() + "'";
    }

    @Override
    public String visit(final SqlLiteralTimestampUtc literal) {
        // TODO Allow dialect to modify behavior
        return "TIMESTAMP '" + literal.getValue().toString() + "'";
    }

    @Override
    public String visit(final SqlLiteralInterval literal) {
        // TODO Allow dialect to modify behavior
        if (literal.getDataType().getIntervalType() == DataType.IntervalType.YEAR_TO_MONTH) {
            return "INTERVAL '" + literal.getValue().toString() + "' YEAR (" + literal.getDataType().getPrecision()
                    + ") TO MONTH";
        } else {
            return "INTERVAL '" + literal.getValue().toString() + "' DAY (" + literal.getDataType().getPrecision()
                    + ") TO SECOND (" + literal.getDataType().getIntervalFraction() + ")";
        }
    }

    @Override
    public String visit(final SqlOrderBy orderBy) throws AdapterException {
        // ORDER BY <expr> [ASC/DESC] [NULLS FIRST/LAST]
        // ASC and NULLS LAST are default in EXASOL
        final List<String> sqlOrderElement = new ArrayList<>();
        for (int i = 0; i < orderBy.getExpressions().size(); ++i) {
            String elementSql = orderBy.getExpressions().get(i).accept(this);
            final boolean shallNullsBeAtTheEnd = orderBy.nullsLast().get(i);
            final boolean isAscending = orderBy.isAscending().get(i);
            if (isAscending == false) {
                elementSql += " DESC";
            }
            if (shallNullsBeAtTheEnd != nullsAreAtEndByDefault(isAscending, this.dialect.getDefaultNullSorting())) {
                // we have to specify null positioning explicitly, otherwise it would be wrong
                elementSql += (shallNullsBeAtTheEnd) ? " NULLS LAST" : " NULLS FIRST";
            }
            sqlOrderElement.add(elementSql);
        }
        return "ORDER BY " + Joiner.on(", ").join(sqlOrderElement);
    }

    /**
     * @param isAscending        true if the desired sort order is ascending, false
     *                           if descending
     * @param defaultNullSorting default null sorting of dialect
     * @return true, if the data source would position nulls at end of the resultset
     *         if NULLS FIRST/LAST is not specified explicitly.
     */
    private boolean nullsAreAtEndByDefault(final boolean isAscending, final SqlDialect.NullSorting defaultNullSorting) {
        if (defaultNullSorting == SqlDialect.NullSorting.NULLS_SORTED_AT_END) {
            return true;
        } else if (defaultNullSorting == SqlDialect.NullSorting.NULLS_SORTED_AT_START) {
            return false;
        } else {
            if (isAscending) {
                return (defaultNullSorting == SqlDialect.NullSorting.NULLS_SORTED_HIGH);
            } else {
                return !(defaultNullSorting == SqlDialect.NullSorting.NULLS_SORTED_HIGH);
            }
        }
    }

    @Override
    public String visit(final SqlPredicateAnd predicate) throws AdapterException {
        final List<String> operandsSql = new ArrayList<>();
        for (final SqlNode node : predicate.getAndedPredicates()) {
            operandsSql.add(node.accept(this));
        }
        return "(" + Joiner.on(" AND ").join(operandsSql) + ")";
    }

    @Override
    public String visit(final SqlPredicateBetween predicate) throws AdapterException {
        return predicate.getExpression().accept(this) + " BETWEEN " + predicate.getBetweenLeft().accept(this) + " AND "
                + predicate.getBetweenRight().accept(this);
    }

    @Override
    public String visit(final SqlPredicateEqual predicate) throws AdapterException {
        return predicate.getLeft().accept(this) + " = " + predicate.getRight().accept(this);
    }

    @Override
    public String visit(final SqlPredicateInConstList predicate) throws AdapterException {
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : predicate.getInArguments()) {
            argumentsSql.add(node.accept(this));
        }
        return predicate.getExpression().accept(this) + " IN (" + Joiner.on(", ").join(argumentsSql) + ")";
    }

    @Override
    public String visit(final SqlPredicateLess predicate) throws AdapterException {
        return predicate.getLeft().accept(this) + " < " + predicate.getRight().accept(this);
    }

    @Override
    public String visit(final SqlPredicateLessEqual predicate) throws AdapterException {
        return predicate.getLeft().accept(this) + " <= " + predicate.getRight().accept(this);
    }

    @Override
    public String visit(final SqlPredicateLike predicate) throws AdapterException {
        String sql = predicate.getLeft().accept(this) + " LIKE " + predicate.getPattern().accept(this);
        if (predicate.getEscapeChar() != null) {
            sql += " ESCAPE " + predicate.getEscapeChar().accept(this);
        }
        return sql;
    }

    @Override
    public String visit(final SqlPredicateLikeRegexp predicate) throws AdapterException {
        return predicate.getLeft().accept(this) + " REGEXP_LIKE " + predicate.getPattern().accept(this);
    }

    @Override
    public String visit(final SqlPredicateNot predicate) throws AdapterException {
        // "SELECT NOT NOT TRUE" is invalid syntax, "SELECT NOT (NOT TRUE)" works.
        return "NOT (" + predicate.getExpression().accept(this) + ")";
    }

    @Override
    public String visit(final SqlPredicateNotEqual predicate) throws AdapterException {
        return predicate.getLeft().accept(this) + " <> " + predicate.getRight().accept(this);
    }

    @Override
    public String visit(final SqlPredicateOr predicate) throws AdapterException {
        final List<String> operandsSql = new ArrayList<>();
        for (final SqlNode node : predicate.getOrPredicates()) {
            operandsSql.add(node.accept(this));
        }
        return "(" + Joiner.on(" OR ").join(operandsSql) + ")";
    }

    @Override
    public String visit(final SqlPredicateIsNull predicate) throws AdapterException {
        return predicate.getExpression().accept(this) + " IS NULL";
    }

    @Override
    public String visit(final SqlPredicateIsNotNull predicate) throws AdapterException {
        return predicate.getExpression().accept(this) + " IS NOT NULL";

    }

}
