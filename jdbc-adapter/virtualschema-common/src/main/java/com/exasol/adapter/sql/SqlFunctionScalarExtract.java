package com.exasol.adapter.sql;

public class SqlFunctionScalarExtract extends SqlNode {
    private String dateTime;
    private SqlNode expression;

    public SqlFunctionScalarExtract(String dateTime, SqlNode expression) {
        this.expression = expression;
        this.dateTime = dateTime;
    }

    public String getDateTime() {
        return dateTime;
    }


    public SqlNode getExpression() {
        return expression;
    }
    
    @Override
    public String toSimpleSql() {
        return "EXTRACT (" + dateTime + " FROM " + expression.toSimpleSql() + ")";
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
