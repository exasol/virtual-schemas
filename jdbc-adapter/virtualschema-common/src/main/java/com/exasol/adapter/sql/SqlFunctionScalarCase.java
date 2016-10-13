package com.exasol.adapter.sql;


import java.util.List;

public class SqlFunctionScalarCase extends SqlNode {
    private List<SqlNode> arguments;
    private List<SqlNode> results;
    private SqlNode basis;

    public SqlFunctionScalarCase(List<SqlNode> arguments, List<SqlNode> results, SqlNode basis) {
        this.arguments = arguments;
        this.results = results;
        this.basis = basis;
    }

    public List<SqlNode> getArguments() {
        return arguments;
    }

    public List<SqlNode> getResults() {
        return results;
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
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public String getFunctionName() {
        return "CASE";
    }

    public ScalarFunction getFunction() {
        return ScalarFunction.CASE;
    }
}
