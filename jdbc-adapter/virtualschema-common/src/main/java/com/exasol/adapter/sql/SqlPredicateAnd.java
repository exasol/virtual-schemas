package com.exasol.adapter.sql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;


public class SqlPredicateAnd extends SqlPredicate {
    
    public SqlPredicateAnd(List<SqlNode> andedPredicates) {
        super(andedPredicates, Predicate.AND);
    }
    
    @Override
    public String toSimpleSql() {
        List<String> operandsSql = new ArrayList<>();
        for (SqlNode node : getSons()) {
            operandsSql.add(node.toSimpleSql());
        }
        return "(" + Joiner.on(" AND ").join(operandsSql) + ")";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_AND;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
