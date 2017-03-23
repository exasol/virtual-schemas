package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;

import java.util.Collections;
import java.util.List;

public class SqlFunctionScalarExtract extends SqlNode {
    private String toExtract;
    private List<SqlNode> arguments;

    public SqlFunctionScalarExtract(String toExtract, List<SqlNode> arguments) {
        assert(arguments.size() == 1);
        this.arguments = arguments;
        this.toExtract = toExtract;
        if (this.arguments != null) {
            for (SqlNode node : this.arguments) {
                node.setParent(this);
            }
        }
    }

    public String getToExtract() {
        return toExtract;
    }


    public List<SqlNode> getArguments() {
        if (arguments == null) {
            return null;
        } else {
            return Collections.unmodifiableList(arguments);
        }
    }
    
    @Override
    public String toSimpleSql() {
        assert(arguments.size() == 1 && arguments.get(0) != null);
        return "EXTRACT (" + toExtract + " FROM " + arguments.get(0).toSimpleSql() + ")";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.FUNCTION_SCALAR_EXTRACT;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

    public String getFunctionName() {
        return "EXTRACT";
    }

    public ScalarFunction getFunction() {
        return ScalarFunction.EXTRACT;
    }

}
