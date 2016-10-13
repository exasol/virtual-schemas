package com.exasol.adapter.sql;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

public class SqlPredicateBetween extends SqlPredicate {

    private SqlNode expression;
    private SqlNode betweenLeft;
    private SqlNode betweenRight;
    
    public SqlPredicateBetween(SqlNode expression, SqlNode betweenLeft, SqlNode betweenRight) {
        super(ImmutableList.of(expression, betweenLeft, betweenRight), Predicate.BETWEEN);
        this.expression = expression;
        this.betweenLeft = betweenLeft;
        this.betweenRight = betweenRight;
    }
    
    public SqlNode getExpression() {
        return expression;
    }
    
    public SqlNode getBetweenLeft() {
        return betweenLeft;
    }
    
    public SqlNode getBetweenRight() {
        return betweenRight;
    }
    
    @Override
    public String toSimpleSql() {
        return expression.toSimpleSql() + " BETWEEN " + betweenLeft.toSimpleSql() + " AND " + betweenRight.toSimpleSql();
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_BETWEEN;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
