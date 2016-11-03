package com.exasol.adapter.sql;

import java.util.List;

public class SqlFunctionScalarExtract extends SqlNode {
    private String dateTime;
    private List<SqlNode> arguments;

    public SqlFunctionScalarExtract(String dateTime, List<SqlNode> arguments) {
        assert(arguments.size() == 1);
        this.arguments = arguments;
        this.dateTime = dateTime;
    }

    public String getDateTime() {
        return dateTime;
    }


    public List<SqlNode> getArguments() {
        return arguments;
    }
    
    @Override
    public String toSimpleSql() {
        assert(arguments.size() == 1 && arguments.get(0) != null);
        return "EXTRACT (" + dateTime + " FROM " + arguments.get(0).toSimpleSql() + ")";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.FUNCTION_SCALAR_EXTRACT;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public String getFunctionName() {
        return "EXTRACT";
    }

    public ScalarFunction getFunction() {
        return ScalarFunction.EXTRACT;
    }

}
