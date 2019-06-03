package com.exasol.adapter.dialects.bigquery;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.*;

import java.util.*;

/**
 * this class implements {@link BigQuerySqlDialect} specific logic of SQL Generation Visitor.
 */
public class BigQuerySqlGenerationVisitor extends SqlGenerationVisitor {
    /**
     * Create a new instance of {@link BigQuerySqlGenerationVisitor}.
     * 
     * @param dialect SQL dialect
     * @param context SQL generation context
     */
    public BigQuerySqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    @Override
    public String visit(final SqlOrderBy orderBy) throws AdapterException {
        final List<String> sqlOrderElement = new ArrayList<>();
        for (int i = 0; i < orderBy.getExpressions().size(); ++i) {
            final boolean isAscending = orderBy.isAscending().get(i);
            final String elementSql = orderBy.getExpressions().get(i).accept(this) + (isAscending ? "" : " DESC");
            sqlOrderElement.add(elementSql);
        }
        return "ORDER BY " + String.join(", ", sqlOrderElement);
    }
}
