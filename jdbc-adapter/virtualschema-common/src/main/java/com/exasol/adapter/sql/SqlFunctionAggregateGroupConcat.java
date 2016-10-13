package com.exasol.adapter.sql;

import java.util.ArrayList;
import java.util.List;


public class SqlFunctionAggregateGroupConcat extends SqlNode {

    private AggregateFunction function;
    private boolean distinct;
    private SqlNode concatExpression;
    private String separator;
    // Lists describing the ORDER BY expressions (must all have same length)
    private List<SqlNode> orderByExpressions;
    private List<Boolean> ascendingOrder;
    private List<Boolean> nullsFirstOrder;

    public SqlFunctionAggregateGroupConcat(AggregateFunction function, SqlNode concatExpression,
                                           List<SqlNode> orderByExpressions, boolean distinct,
                                           List<Boolean> ascendingOrder, List<Boolean> nullsFirstOrder,
                                           String separator) {
        assert(orderByExpressions.size() == ascendingOrder.size());
        assert(ascendingOrder.size() == nullsFirstOrder.size());
        List<SqlNode> sons = new ArrayList<>();
        sons.add(concatExpression);
        sons.addAll(orderByExpressions);
        setSons(sons);
        this.function = function;
        this.distinct = distinct;
        this.concatExpression = concatExpression;
        this.orderByExpressions = orderByExpressions;
        this.ascendingOrder = ascendingOrder;
        this.nullsFirstOrder = nullsFirstOrder;
        this.separator = separator;
    }

    public AggregateFunction getFunction() {
        return function;
    }

    public SqlNode getConcatExpression() {
        return concatExpression;
    }

    public List<Boolean> getAscendingOrderList() {
        return ascendingOrder;
    }

    public List<Boolean> getNullsFirstOrderList() {
        return nullsFirstOrder;
    }

    public String getFunctionName() {
        return function.name();
    }

    public String getSeparator() {
        return separator;
    }

    public List<SqlNode> getOrderByExpressions() {
        return orderByExpressions;
    }

    public boolean hasDistinct() {
        return distinct;
    }
    
    @Override
    public String toSimpleSql() {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : getSons()) {
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
        StringBuilder builder = new StringBuilder();
        builder.append(getFunctionName());
        builder.append("(");
        builder.append(distinctSql);
        builder.append(concatExpression.toSimpleSql());
        if (getOrderByExpressions().size() > 0) {
            builder.append(" ORDER BY ");
            for (int i = 0; i < getSons().size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(getSon(i).toSimpleSql());
                if (!getAscendingOrderList().get(i)) {
                    builder.append(" DESC");
                }
                if (getNullsFirstOrderList().get(i)) {
                    builder.append(" NULLS FIRST");
                }
            }
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
