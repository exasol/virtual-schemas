package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;

import java.util.Collections;
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
        assert(arguments != null); // currently the adapter supports only one expression as argument
        assert(arguments.size() == 1 && arguments.get(0) != null);
        this.function = function;
        this.distinct = distinct;
        this.arguments = arguments;
        this.orderBy = orderBy;
        this.separator = separator;
        if (this.arguments != null) {
            for (SqlNode node : this.arguments) {
                node.setParent(this);
            }
        }
        if (orderBy != null) {
            orderBy.setParent(this);
        }
    }

    public AggregateFunction getFunction() {
        return function;
    }

    public List<SqlNode> getArguments() {
        if (arguments == null) {
            return null;
        } else {
            return Collections.unmodifiableList(arguments);
        }
    }

    public boolean hasOrderBy() {
        return orderBy != null && orderBy.getExpressions() != null && orderBy.getExpressions().size() > 0;
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
        assert(arguments != null);
        assert(arguments.size() == 1 && arguments.get(0) != null);
        builder.append(arguments.get(0).toSimpleSql());
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
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
