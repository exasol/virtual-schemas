package com.exasol.adapter.sql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;


public class SqlPredicateOr extends SqlPredicate {
    
    public SqlPredicateOr(List<SqlNode> orPredicates) {
        super(orPredicates, Predicate.OR);
    }
    
    @Override
    public String toSimpleSql() {
        List<String> operandsSql = new ArrayList<>();
        for (SqlNode node : getSons()) {
            operandsSql.add(node.toSimpleSql());
        }
        return "(" + Joiner.on(" OR ").join(operandsSql) + ")";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_OR;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
