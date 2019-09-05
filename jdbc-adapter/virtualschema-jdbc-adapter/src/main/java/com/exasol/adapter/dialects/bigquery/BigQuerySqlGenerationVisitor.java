package com.exasol.adapter.dialects.bigquery;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.SqlOrderBy;

/**
 * This class implements a Google-Big-Query-specific variant of an SQL generation visitor.
 */
public class BigQuerySqlGenerationVisitor extends SqlGenerationVisitor {
    /**
     * Create a new instance of the {@link BigQuerySqlGenerationVisitor}.
     * 
     * @param dialect Big Query SQL dialect
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
