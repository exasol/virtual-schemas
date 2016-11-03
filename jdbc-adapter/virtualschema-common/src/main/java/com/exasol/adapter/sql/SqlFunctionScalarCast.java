package com.exasol.adapter.sql;

import com.exasol.adapter.metadata.DataType;

import java.util.List;

public class SqlFunctionScalarCast extends SqlNode {
    private DataType dataType;
    private List<SqlNode> arguments;

    public SqlFunctionScalarCast(DataType dataType, List<SqlNode> arguments) {
        assert(arguments.size() == 1);
        this.arguments = arguments;
        this.dataType = dataType;
    }

    public DataType getDataType() {
        return dataType;
    }


    public List<SqlNode> getArguments() {
        return arguments;
    }
    
    @Override
    public String toSimpleSql() {
        assert(arguments.size() == 1 && arguments.get(0) != null);
        return "CAST (" + arguments.get(0).toSimpleSql() + " AS " + getDataType().toString() + ")";
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
