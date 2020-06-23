package com.exasol.adapter.dialects.mysql;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.*;

public class MySqlSqlGenerationVisitor extends SqlGenerationVisitor {
    public MySqlSqlGenerationVisitor(final SqlDialect dialect, final SqlGenerationContext context) {
        super(dialect, context);
    }

    @Override
    public String visit(final SqlFunctionScalar function) throws AdapterException {
        if (function.getFunction() == ScalarFunction.DIV) {
            return getChangedDiv(function);
        }
        return super.visit(function);
    }

    private String getChangedDiv(final SqlFunctionScalar function) throws AdapterException {
        final List<SqlNode> arguments = function.getArguments();
        final List<String> argumentsSql = new ArrayList<>(arguments.size());
        for (final SqlNode node : arguments) {
            argumentsSql.add(node.accept(this));
        }
        return argumentsSql.get(0) + " DIV " + argumentsSql.get(1);
    }
}