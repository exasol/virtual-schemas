package com.exasol.adapter.dialects;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;

/**
 * This class has the logic to generate SQL queries based on a graph of {@link SqlNode} elements. It uses the visitor
 * pattern. This class interacts with the dialects in some situations, e.g. to find out how to handle quoting,
 * case-sensitivity.
 *
 * <p>
 * If this class is not sufficiently customizable for your use case, you can extend this class and override the required
 * methods. You also have to return your custom visitor class then in the method
 * {@link SqlDialect#getSqlGenerationVisitor(SqlGenerationContext)}. See
 * {@link com.exasol.adapter.dialects.oracle.OracleSqlGenerationVisitor} for an example.
 * </p>
 *
 * Note on operator associativity and parenthesis generation: Currently we use parenthesis almost always. Without
 * parenthesis, two SqlNode graphs with different semantic lead to "select 1 = 1 - 1 + 1". Also "SELECT NOT NOT TRUE"
 * needs to be written as "SELECT NOT (NOT TRUE)" to work at all, whereas SELECT NOT TRUE works fine without
 * parentheses. Currently we make inflationary use of parenthesis to to enforce the right semantic, but hopefully there
 * is a better way.
 */
public class SqlGenerationVisitor implements SqlNodeVisitor<String> {
    private final SqlDialect dialect;
    private final SqlGenerationContext context;

    /**
     * Creates a new instance of the {@link SqlGenerationVisitor}.
     *
     * @param dialect SQl dialect
     * @param context SQL generation context
     */
    public SqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        this.dialect = dialect;
        this.context = context;
        checkDialectAliases();
    }

    protected SqlDialect getDialect() {
        return this.dialect;
    }

    protected void checkDialectAliases() {
        // Check if dialect provided invalid aliases, which would never be applied.
        for (final ScalarFunction function : this.dialect.getScalarFunctionAliases().keySet()) {
            if (!function.isSimple()) {
                throw new UnsupportedOperationException("The dialect " + this.dialect.getName()
                        + " provided an alias for the non-simple scalar function " + function.name()
                        + ". This alias will never be considered.");
            }
        }
        for (final AggregateFunction function : this.dialect.getAggregateFunctionAliases().keySet()) {
            if (!function.isSimple()) {
                throw new UnsupportedOperationException("The dialect " + this.dialect.getName()
                        + " provided an alias for the non-simple aggregate function " + function.name()
                        + ". This alias will never be considered.");
            }
        }
    }

    protected boolean isDirectlyInSelectList(final SqlColumn column) {
        return column.hasParent() && (column.getParent().getType() == SqlNodeType.SELECT_LIST);
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
        if (selectList.isRequestAnyColumn()) {
            return representAnyColumnInSelectList();
        } else if (selectList.isSelectStar()) {
            return representAsteriskInSelectList(selectList);
        } else {
            return createExplicitColumnsSelectList(selectList);
        }
    }

    /**
     * Represent "any column" in the <code>SELECT</code> list.
     * <p>
     * A typical example are queries where the only thing that matters if what you searched for exists or not. Different
     * databases have different preferred ways of expressing this kind of result.
     * </p>
     *
     * @return always <code>"true"</code>
     */
    protected String representAnyColumnInSelectList() {
        return SqlConstants.TRUE;
    }

    /**
     * Represent "all columns" (the asterisk) in the <code>SELECT</code> list.
     * <p>
     * In most cases simply taking over the original asterisk will be sufficient and is therefore implemented in this
     * base class.
     * </p>
     * <p>
     * Override this method in case conversions are necessary.
     * </p>
     *
     * @param selectList list of columns (or expressions) in the <code>SELECT</code> part
     * @return always <code>"true"</code>
     */
    protected String representAsteriskInSelectList(final SqlSelectList selectList) throws AdapterException {
        return SqlConstants.ASTERISK;
    }

    /**
     * Create a list of explicitly specified columns (where columns can also be expressions) for a <code>SELECT</code>
     * list.
     *
     * @param selectList list of columns (or expressions) in the <code>SELECT</code> part
     * @return string representing the <code>SELECT</code> list
     * @throws AdapterException in case the expressions in the list can't be rendered to SQL
     */
    protected String createExplicitColumnsSelectList(final SqlSelectList selectList) throws AdapterException {
        final List<SqlNode> expressions = selectList.getExpressions();
        final List<String> selectElement = new ArrayList<>(expressions.size());
        for (final SqlNode node : expressions) {
            selectElement.add(node.accept(this));
        }
        return String.join(", ", selectElement);
    }

    @Override
    public String visit(final SqlColumn column) throws AdapterException {
        String tablePrefix = "";
        if (column.hasTableAlias()) {
            tablePrefix = this.dialect.applyQuote(column.getTableAlias())
                    + this.dialect.getTableCatalogAndSchemaSeparator();
        } else if ((column.getTableName() != null) && !column.getTableName().isEmpty()) {
            tablePrefix = this.dialect.applyQuote(column.getTableName())
                    + this.dialect.getTableCatalogAndSchemaSeparator();
        }
        return tablePrefix + this.dialect.applyQuote(column.getName());
    }

    @Override
    public String visit(final SqlTable table) {
        String schemaPrefix = "";
        if (this.dialect.requiresCatalogQualifiedTableNames(this.context) && (this.context.getCatalogName() != null)
                && !this.context.getCatalogName().isEmpty()) {
            schemaPrefix = this.dialect.applyQuote(this.context.getCatalogName())
                    + this.dialect.getTableCatalogAndSchemaSeparator();
        }
        if (this.dialect.requiresSchemaQualifiedTableNames(this.context) && (this.context.getSchemaName() != null)
                && !this.context.getSchemaName().isEmpty()) {
            schemaPrefix += this.dialect.applyQuote(this.context.getSchemaName())
                    + this.dialect.getTableCatalogAndSchemaSeparator();
        }
        if (table.hasAlias()) {
            return schemaPrefix + this.dialect.applyQuote(table.getName()) + " "
                    + this.dialect.applyQuote(table.getAlias());
        } else {
            return schemaPrefix + this.dialect.applyQuote(table.getName());
        }
    }

    @Override
    public String visit(final SqlJoin join) throws AdapterException {
        return join.getLeft().accept(this) + " " + join.getJoinType().name().replace('_', ' ') + " JOIN "
                + join.getRight().accept(this) + " ON " + join.getCondition().accept(this);
    }

    @Override
    public String visit(final SqlGroupBy groupBy) throws AdapterException {
        if ((groupBy.getExpressions() == null) || groupBy.getExpressions().isEmpty()) {
            throw new IllegalStateException("Unexpected internal state (empty group by)");
        }
        final List<String> selectElement = new ArrayList<>();
        for (final SqlNode node : groupBy.getExpressions()) {
            selectElement.add(node.accept(this));
        }
        return String.join(", ", selectElement);
    }

    @Override
    public String visit(final SqlFunctionAggregate function) throws AdapterException {
        final List<String> argumentsSql = new ArrayList<>();
        for (final SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        if (function.getFunctionName().equalsIgnoreCase("count") && argumentsSql.isEmpty()) {
            argumentsSql.add(SqlConstants.ASTERISK);
        }
        String distinctSql = "";
        if (function.hasDistinct()) {
            distinctSql = "DISTINCT ";
        }
        String functionNameInSourceSystem = function.getFunctionName();
        if (this.dialect.getAggregateFunctionAliases().containsKey(function.getFunction())) {
            functionNameInSourceSystem = this.dialect.getAggregateFunctionAliases().get(function.getFunction());
        }
        return functionNameInSourceSystem + "(" + distinctSql + String.join(", ", argumentsSql) + ")";
    }

    @Override
    public String visit(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        validateSingleAgrumentFunctionParameter(function);
        final StringBuilder builder = new StringBuilder();
        builder.append(function.getFunctionName());
        builder.append("(");
        if (function.hasDistinct()) {
            builder.append("DISTINCT ");
        }
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

    private void validateSingleAgrumentFunctionParameter(final SqlFunctionAggregateGroupConcat function) {
        if ((function.getArguments().size() != 1) || (function.getArguments().get(0) == null)) {
            throw new IllegalArgumentException(
                    "Function AGGREGATE GROUP CONCAT must have exactly one non-NULL parameter.");
        }
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
        if (argumentsSql.isEmpty() && this.dialect.omitParentheses(function.getFunction())) {
            return functionNameInSourceSystem;
        } else {
            return functionNameInSourceSystem + "(" + String.join(", ", argumentsSql) + ")";
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
        validateSingleAgrumentFunctionParameter(function);
        final StringBuilder builder = new StringBuilder();
        builder.append("CAST");
        builder.append("(");
        builder.append(function.getArguments().get(0).accept(this));
        builder.append(" AS ");
        builder.append(function.getDataType());
        builder.append(")");
        return builder.toString();
    }

    private void validateSingleAgrumentFunctionParameter(final SqlFunctionScalarCast function) {
        if ((function.getArguments().size() != 1) || (function.getArguments().get(0) == null)) {
            throw new IllegalArgumentException("Function CAST must have exactly one non-NULL parameter.");
        }
    }

    @Override
    public String visit(final SqlFunctionScalarExtract function) throws AdapterException {
        validateSingleAgrumentFunctionParameter(function);
        final String expression = function.getArguments().get(0).accept(this);
        return function.getFunctionName() + "(" + function.getToExtract() + " FROM " + expression + ")";
    }

    private void validateSingleAgrumentFunctionParameter(final SqlFunctionScalarExtract function) {
        if ((function.getArguments().size() != 1) || (function.getArguments().get(0) == null)) {
            throw new IllegalArgumentException("Function EXTRACT must have exactly one non-NULL parameter.");
        }
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
            return SqlConstants.TRUE;
        } else {
            return SqlConstants.FALSE;
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
        return "TIMESTAMP '" + literal.getValue() + "'";
    }

    @Override
    public String visit(final SqlLiteralTimestampUtc literal) {
        return "TIMESTAMP '" + literal.getValue() + "'";
    }

    @Override
    public String visit(final SqlLiteralInterval literal) {
        if (literal.getDataType().getIntervalType() == DataType.IntervalType.YEAR_TO_MONTH) {
            return "INTERVAL '" + literal.getValue() + "' YEAR (" + literal.getDataType().getPrecision() + ") TO MONTH";
        } else {
            return "INTERVAL '" + literal.getValue() + "' DAY (" + literal.getDataType().getPrecision()
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
            if (!isAscending) {
                elementSql += " DESC";
            }
            if (shallNullsBeAtTheEnd != nullsAreAtEndByDefault(isAscending, this.dialect.getDefaultNullSorting())) {
                // we have to specify null positioning explicitly, otherwise it would be wrong
                elementSql += (shallNullsBeAtTheEnd) ? " NULLS LAST" : " NULLS FIRST";
            }
            sqlOrderElement.add(elementSql);
        }
        return "ORDER BY " + String.join(", ", sqlOrderElement);
    }

    /**
     * @param isAscending        true if the desired sort order is ascending, false if descending
     * @param defaultNullSorting default null sorting of dialect
     * @return true, if the data source would position nulls at end of the resultset if NULLS FIRST/LAST is not
     *         specified explicitly.
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
                return (defaultNullSorting != SqlDialect.NullSorting.NULLS_SORTED_HIGH);
            }
        }
    }

    @Override
    public String visit(final SqlPredicateAnd predicate) throws AdapterException {
        final List<String> operandsSql = new ArrayList<>();
        for (final SqlNode node : predicate.getAndedPredicates()) {
            operandsSql.add(node.accept(this));
        }
        return "(" + String.join(" AND ", operandsSql) + ")";
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
        return predicate.getExpression().accept(this) + " IN (" + String.join(", ", argumentsSql) + ")";
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
        return "(" + String.join(" OR ", operandsSql) + ")";
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