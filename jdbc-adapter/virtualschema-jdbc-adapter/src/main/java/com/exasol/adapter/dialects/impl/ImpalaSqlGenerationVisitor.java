package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.sql.*;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

public class ImpalaSqlGenerationVisitor extends SqlGenerationVisitor {

    SqlDialect dialect;

    public ImpalaSqlGenerationVisitor(SqlDialect dialect, SqlGenerationContext context) {
        super(dialect, context);
        this.dialect = dialect;
    }

    @Override
    public String visit(SqlPredicateLikeRegexp predicate) throws AdapterException {
        return predicate.getLeft().accept(this) + " REGEXP "
                + predicate.getPattern().accept(this);
    }

    @Override
    public String visit(SqlFunctionAggregateGroupConcat function) throws AdapterException {
        // Note that GROUP_CONCAT with DISTINCT is not supported by Impala
        StringBuilder builder = new StringBuilder();
        builder.append(function.getFunctionName());
        builder.append("(");
        // To use it group_concat with numeric values we would need to sync group_concat(cast(x as string)). Since we cannot compute the type, we always cast
        builder.append("CAST(");
        assert(function.getArguments() != null);
        assert(function.getArguments().size() == 1 && function.getArguments().get(0) != null);
        builder.append(function.getArguments().get(0).accept(this));
        builder.append(" AS STRING)");
        if (function.getSeparator() != null) {
            builder.append(", ");
            builder.append("'");
            builder.append(function.getSeparator());
            builder.append("'");
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String visit(SqlFunctionAggregate function) throws AdapterException {
        boolean isDirectlyInSelectList = (function.hasParent() && function.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (function.getFunction() != AggregateFunction.SUM || !isDirectlyInSelectList) {
            return super.visit(function);
        } else {
            // For SUM, the JDBC driver returns type DOUBLE in prepared statement but the actual
            // query returns DECIMAL in ResultSetMetadata, so that IMPORT fails. Casting to DOUBLE
            // solves the problem.
            List<String> argumentsSql = new ArrayList<>();
            for (SqlNode node : function.getArguments()) {
                argumentsSql.add(node.accept(this));
            }
            String distinctSql = "";
            if (function.hasDistinct()) {
                distinctSql = "DISTINCT ";
            }
            String functionNameInSourceSystem = function.getFunctionName();
            if (dialect.getAggregateFunctionAliases().containsKey(function.getFunction())) {
                functionNameInSourceSystem = dialect.getAggregateFunctionAliases().get(function.getFunction());
            }
            return "CAST(" + functionNameInSourceSystem + "(" + distinctSql
                    + Joiner.on(", ").join(argumentsSql) + ") AS DOUBLE)";
        }
    }
}
