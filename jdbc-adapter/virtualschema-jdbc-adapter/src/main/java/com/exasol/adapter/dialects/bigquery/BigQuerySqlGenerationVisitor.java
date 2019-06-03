package com.exasol.adapter.dialects.bigquery;

import com.exasol.adapter.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.*;

import java.util.*;

public class BigQuerySqlGenerationVisitor extends SqlGenerationVisitor {
    public BigQuerySqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    @Override
    public String visit(final SqlOrderBy orderBy) throws AdapterException {
        final List<String> sqlOrderElement = new ArrayList<>();
        for (int i = 0; i < orderBy.getExpressions().size(); ++i) {
            String elementSql = orderBy.getExpressions().get(i).accept(this);
            final boolean isAscending = orderBy.isAscending().get(i);
            if (!isAscending) {
                elementSql += " DESC";
            }
            sqlOrderElement.add(elementSql);
        }
        return "ORDER BY " + String.join(", ", sqlOrderElement);
    }
}
