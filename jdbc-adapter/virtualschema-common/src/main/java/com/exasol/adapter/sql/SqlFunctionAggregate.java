package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A simple aggregate function with a name and zero or more arguments. Distinct is also allowed.
 *
 * <p>Aggregate functions that are more complex, like GroupConcat, are defined in separate classes.
 * </p>
 */
public class SqlFunctionAggregate extends SqlNode {

    private AggregateFunction function;
    private boolean distinct;
    private List<SqlNode> arguments;

    public SqlFunctionAggregate(AggregateFunction function, List<SqlNode> arguments, boolean distinct) {
        this.arguments = arguments;
        this.function = function;
        this.distinct = distinct;
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


    public AggregateFunction getFunction() {
        return function;
    }
    
    public String getFunctionName() {
        return function.name();
    }

    public boolean hasDistinct() {
        return distinct;
    }
    
    @Override
    public String toSimpleSql() {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : arguments) {
            argumentsSql.add(node.toSimpleSql());
        }
        if (argumentsSql.size() == 0) {
            assert(getFunctionName().equalsIgnoreCase("count"));
            argumentsSql.add("*");
        }
        String distinctSql = "";
        if (distinct) {
            distinctSql = "DISTINCT ";
        }
        return getFunctionName() + "(" + distinctSql + Joiner.on(", ").join(argumentsSql) + ")";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.FUNCTION_AGGREGATE;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
