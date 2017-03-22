package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.sql.SqlFunctionAggregateGroupConcat;

public class RedshiftSqlGenerationVisitor extends SqlGenerationVisitor {

    public RedshiftSqlGenerationVisitor(SqlDialect dialect, SqlGenerationContext context) {
        super(dialect, context);
      
    }
 
    @Override
    public String visit(SqlFunctionAggregateGroupConcat function) throws AdapterException {
        StringBuilder builder = new StringBuilder();
        builder.append("LISTAGG");
        builder.append("(");
        assert(function.getArguments() != null);
        assert(function.getArguments().size() == 1 && function.getArguments().get(0) != null);
        String expression = function.getArguments().get(0).accept(this);
        builder.append(expression);
        builder.append(", ");
        String separator = ",";
        if (function.getSeparator() != null) {
            separator = function.getSeparator();
        }
        builder.append("'");
        builder.append(separator);
        builder.append("') ");
        builder.append("WITHIN GROUP(ORDER BY ");
        if (function.hasOrderBy()) {
            for (int i = 0; i < function.getOrderBy().getExpressions().size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(function.getOrderBy().getExpressions().get(i).accept(this));
                if (!function.getOrderBy().isAscending().get(i)) {
                    builder.append(" DESC");
                }
                if (!function.getOrderBy().nullsLast().get(i)) {
                    builder.append(" NULLS FIRST");
                }
            }
        } else {
            builder.append(expression);
        }
        builder.append(")");
        return builder.toString();
    }

   
}