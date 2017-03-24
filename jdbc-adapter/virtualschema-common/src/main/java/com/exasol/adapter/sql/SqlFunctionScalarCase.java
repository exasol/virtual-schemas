package com.exasol.adapter.sql;


import com.exasol.adapter.AdapterException;

import java.util.Collections;
import java.util.List;

public class SqlFunctionScalarCase extends SqlNode {
    private List<SqlNode> arguments;
    private List<SqlNode> results;
    private SqlNode basis;

    public SqlFunctionScalarCase(List<SqlNode> arguments, List<SqlNode> results, SqlNode basis) {
        this.arguments = arguments;
        this.results = results;
        this.basis = basis;
        if (this.arguments != null) {
            for (SqlNode node : this.arguments) {
                node.setParent(this);
            }
        }
        if (this.results != null) {
            for (SqlNode node : this.results) {
                node.setParent(this);
            }
        }
        if (basis != null) {
            basis.setParent(this);
        }
    }

    public List<SqlNode> getArguments() {
        if (arguments == null) {
            return null;
        } else {
            return Collections.unmodifiableList(arguments);
        }
    }

    public List<SqlNode> getResults() {
        if (results == null) {
            return null;
        } else {
            return Collections.unmodifiableList(results);
        }
    }

    public SqlNode getBasis() {
        return basis;
    }

    @Override
    public String toSimpleSql() {
        return "CASE";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.FUNCTION_SCALAR_CASE;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

    public String getFunctionName() {
        return "CASE";
    }

    public ScalarFunction getFunction() {
        return ScalarFunction.CASE;
    }
}
