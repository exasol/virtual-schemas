package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class SqlPredicateAnd extends SqlPredicate {

    List<SqlNode> andedPredicates;

    public SqlPredicateAnd(List<SqlNode> andedPredicates) {
        super(Predicate.AND);
        this.andedPredicates = andedPredicates;
        if (this.andedPredicates != null) {
            for (SqlNode node : this.andedPredicates) {
                node.setParent(this);
            }
        }
    }

    public List<SqlNode> getAndedPredicates() {
        if (andedPredicates == null) {
            return null;
        } else {
            return Collections.unmodifiableList(andedPredicates);
        }
    }

    @Override
    public String toSimpleSql() {
        List<String> operandsSql = new ArrayList<>();
        for (SqlNode node : andedPredicates) {
            operandsSql.add(node.toSimpleSql());
        }
        return "(" + Joiner.on(" AND ").join(operandsSql) + ")";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_AND;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
