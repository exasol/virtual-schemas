package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.google.common.base.Joiner;

import java.util.*;

/**
 * A simple scalar function with a name and zero or more arguments.
 *
 * <p>Scalar functions that are more complex, like CASE or CAST, are defined in separate classes.
 * </p>
 */
public class SqlFunctionScalar extends SqlNode {

    private List<SqlNode> arguments;
    private ScalarFunction function;
    private boolean isInfix;
    private boolean isPrefix;

    public SqlFunctionScalar(ScalarFunction function, List<SqlNode> arguments, boolean isInfix, boolean isPrefix) {
        this.arguments = arguments;
        this.function = function;
        this.isInfix = isInfix;
        this.isPrefix = isPrefix;
        if (this.arguments != null) {
            for (SqlNode node : this.arguments) {
                node.setParent(this);
            }
        }
    }

    public List<SqlNode> getArguments() {
        if (arguments == null) {
            return null;
        } else {
            return Collections.unmodifiableList(arguments);
        }
    }

    public ScalarFunction getFunction() {
        return function;
    }

    public String getFunctionName() {
        return function.name();
    }
    
    public int getNumArgs() {
        return getArguments().size();
    }
    
    public boolean isInfix() {
        return isInfix;
    }

    public boolean isPrefix() {
        return isPrefix;
    }


    @Override
    public String toSimpleSql() {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : arguments) {
            argumentsSql.add(node.toSimpleSql());
        }
        if (isInfix) {
            assert(argumentsSql.size()==2);
            Map<String, String> functionAliases = new HashMap<String, String>();
            functionAliases.put("ADD", "+");
            functionAliases.put("SUB", "-");
            functionAliases.put("MULT", "*");
            functionAliases.put("FLOAT_DIV", "/");
            String realFunctionName = getFunctionName();
            if (functionAliases.containsKey(getFunctionName())) {
                realFunctionName = functionAliases.get(getFunctionName());
            }
            return "(" + argumentsSql.get(0) + " " + realFunctionName + " " + argumentsSql.get(1) + ")";
        } else if (isPrefix) {
            assert(argumentsSql.size()==1);
            Map<String, String> functionAliases = new HashMap<String, String>();
            functionAliases.put("NEG", "-");
            String realFunctionName = getFunctionName();
            if (functionAliases.containsKey(getFunctionName())) {
                realFunctionName = functionAliases.get(getFunctionName());
            }
            return "(" + realFunctionName + argumentsSql.get(1) + ")";
        }
        return getFunctionName() + "(" + Joiner.on(", ").join(argumentsSql) + ")";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.FUNCTION_SCALAR;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
