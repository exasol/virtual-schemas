package com.exasol.adapter.sql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;


public class SqlPredicateInConstList extends SqlPredicate {
    
    // For <exp> IN (...) this stores <exp>
    SqlNode expression;
    // Arguments inside the brackets
    List<SqlNode> inArguments;
    
    public SqlPredicateInConstList(SqlNode expression, List<SqlNode> inArguments) {
        super(ImmutableList.<SqlNode>builder().add(expression).addAll(inArguments).build(), Predicate.IN_CONSTLIST);
        this.expression = expression;
        this.inArguments = inArguments;
    }
    
    public SqlNode getExpression() {
        return expression;
    }
    
    public List<SqlNode> getInArguments() {
        return inArguments;
    }
    
    @Override
    public String toSimpleSql() {
        List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : inArguments) {
            argumentsSql.add(node.toSimpleSql());
        }
        return expression.toSimpleSql() + " IN (" + Joiner.on(", ").join(argumentsSql) + ")";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_IN_CONSTLIST;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
