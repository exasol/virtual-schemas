package com.exasol.adapter.dialects.impala;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.*;

/**
 * This class generates SQL queries for the {@link ImpalaSqlDialect}.
 */
public class ImpalaSqlGenerationVisitor extends SqlGenerationVisitor {
    private final SqlDialect dialect;

    /**
     * Create a new instance of the {@link ImpalaSqlGenerationVisitor}.
     *
     * @param dialect {@link ImpalaSqlDialect} SQL dialect
     * @param context SQL generation context
     */
    public ImpalaSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
        this.dialect = dialect;
    }

    @Override
    public String visit(final SqlPredicateLikeRegexp predicate) throws AdapterException {
        return predicate.getLeft().accept(this) + " REGEXP " + predicate.getPattern().accept(this);
    }

    @Override
    public String visit(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        // Note that GROUP_CONCAT with DISTINCT is not supported by Impala
        final StringBuilder builder = new StringBuilder();
        builder.append(function.getFunctionName());
        builder.append("(");
        // To use it group_concat with numeric values we would need to sync group_concat(cast(x as string)). Since we
        // cannot compute the type, we always cast
        builder.append("CAST(");
        builder.append(function.getArgument().accept(this));
        builder.append(" AS STRING)");
        if (function.hasSeparator()) {
            builder.append(", ");
            builder.append(function.getSeparator().accept(this));
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String visit(final SqlFunctionAggregate function) throws AdapterException {
        final boolean isDirectlyInSelectList = (function.hasParent()
                && function.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (function.getFunction() != AggregateFunction.SUM || !isDirectlyInSelectList) {
            return super.visit(function);
        } else {
            // For SUM, the JDBC driver returns type DOUBLE in prepared statement but the actual
            // query returns DECIMAL in ResultSetMetadata, so that IMPORT fails. Casting to DOUBLE
            // solves the problem.
            final List<SqlNode> arguments = function.getArguments();
            final List<String> argumentsSql = new ArrayList<>(arguments.size());
            for (final SqlNode node : arguments) {
                argumentsSql.add(node.accept(this));
            }
            String distinctSql = "";
            if (function.hasDistinct()) {
                distinctSql = "DISTINCT ";
            }
            String functionNameInSourceSystem = function.getFunctionName();
            if (this.dialect.getAggregateFunctionAliases().containsKey(function.getFunction())) {
                functionNameInSourceSystem = this.dialect.getAggregateFunctionAliases().get(function.getFunction());
            }
            return "CAST(" + functionNameInSourceSystem + "(" + distinctSql + String.join(", ", argumentsSql)
                    + ") AS DOUBLE)";
        }
    }
}