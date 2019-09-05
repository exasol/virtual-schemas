package com.exasol.adapter.dialects.redshift;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.SqlFunctionAggregateGroupConcat;

/**
 * This class generates SQL queries for the {@link RedshiftSqlGenerationVisitor}.
 */
public class RedshiftSqlGenerationVisitor extends SqlGenerationVisitor {

    /**
     * Create a new instance of the {@link RedshiftSqlGenerationVisitor}.
     *
     * @param dialect {@link RedshiftSqlDialect} SQL dialect
     * @param context SQL generation context
     */
    public RedshiftSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }
 
    @Override
    public String visit(final SqlFunctionAggregateGroupConcat function) throws AdapterException {
        final StringBuilder builder = new StringBuilder();
        builder.append("LISTAGG");
        builder.append("(");
        assert(function.getArguments() != null);
        assert(function.getArguments().size() == 1 && function.getArguments().get(0) != null);
        final String expression = function.getArguments().get(0).accept(this);
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