package com.exasol.adapter.sql;

import java.util.ArrayList;
import java.util.List;


public class SqlFunctionAggregateGroupConcat extends SqlNode {

    private AggregateFunction function;
    private boolean distinct;
    private List<SqlNode> arguments;
    private String separator;
    private SqlOrderBy orderBy;



    public SqlFunctionAggregateGroupConcat(AggregateFunction function, List<SqlNode> arguments,
                                           SqlOrderBy orderBy, boolean distinct,
                                           String separator) {
        assert(arguments.size() == 1); // currently the adapter supports only one expression
        List<SqlNode> sons = new ArrayList<>();
        if (arguments != null && arguments.size() > 0) {
            sons.addAll(arguments);
        }
        if (orderBy != null) {
            sons.add(orderBy);
        }
        setSons(sons);
        this.function = function;
        this.distinct = distinct;
        this.arguments = arguments;
        this.orderBy = orderBy;
        this.separator = separator;
    }

    public AggregateFunction getFunction() {
        return function;
    }

    public List<SqlNode> getArguments() {
        return arguments;
    }

    public SqlOrderBy getOrderBy() {
        return orderBy;
    }

    public String getFunctionName() {
        return function.name();
    }

    public String getSeparator() {
        return separator;
    }

    public boolean hasDistinct() {
        return distinct;
    }
    
    @Override
    public String toSimpleSql() {
        String distinctSql = "";
        if (distinct) {
            distinctSql = "DISTINCT ";
        }
        StringBuilder builder = new StringBuilder();
        builder.append(getFunctionName());
        builder.append("(");
        builder.append(distinctSql);
        assert(getArguments().size() == 1 && getArguments().get(0) != null);
        builder.append(getArguments().get(0).toSimpleSql());
        if (orderBy != null) {
            builder.append(" ");
            builder.append(orderBy.toSimpleSql());
        }
        if (separator != null) {
            builder.append(" SEPARATOR ");
            builder.append("'");
            builder.append(separator);
            builder.append("'");
        }
        builder.append(")");
        return builder.toString();
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.FUNCTION_AGGREGATE;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
