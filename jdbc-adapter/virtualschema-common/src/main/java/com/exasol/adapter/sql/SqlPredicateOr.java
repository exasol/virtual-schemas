package com.exasol.adapter.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Joiner;


public class SqlPredicateOr extends SqlPredicate {

    List<SqlNode> orPredicates;

    public SqlPredicateOr(List<SqlNode> orPredicates) {
        super(Predicate.OR);
        this.orPredicates  = orPredicates;
        if (this.orPredicates != null) {
            for (SqlNode node : this.orPredicates) {
                node.setParent(this);
            }
        }
    }

    public List<SqlNode> getOrPredicates() {
        if (orPredicates == null) {
            return null;
        } else {
            return Collections.unmodifiableList(orPredicates);
        }
    }

    @Override
    public String toSimpleSql() {
        List<String> operandsSql = new ArrayList<>();
        for (SqlNode node : orPredicates) {
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
