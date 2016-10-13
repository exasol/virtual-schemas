package com.exasol.adapter.sql;

import com.exasol.adapter.metadata.DataType;

public class SqlFunctionScalarCast extends SqlNode {
    private DataType dataType;
    private SqlNode expression;

    public SqlFunctionScalarCast(DataType dataType, SqlNode expression) {
        this.expression = expression;
        this.dataType = dataType;
    }

    public DataType getDataType() {
        return dataType;
    }


    public SqlNode getExpression() {
        return expression;
    }
    
    @Override
    public String toSimpleSql() {
        return "CAST (" + expression.toSimpleSql() + " AS " + getDataType().toString() + ")";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.FUNCTION_SCALAR_CAST;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    public String getFunctionName() {
        return "CAST";
    }

    public ScalarFunction getFunction() {
        return ScalarFunction.CAST;
    }

}
