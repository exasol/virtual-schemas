package com.exasol.adapter.sql;


import com.google.common.collect.ImmutableList;

public class SqlPredicateIsNull extends SqlPredicate {

    private SqlNode expression;

    public SqlPredicateIsNull(SqlNode expression) {
        super(Predicate.IS_NULL);
        this.expression = expression;
        if (this.expression != null) {
            this.expression.setParent(this);
        }
    }
    
    public SqlNode getExpression() {
        return expression;
    }
    
    @Override
    public String toSimpleSql() {
        return expression.toSimpleSql() + " IS NULL";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_IS_NULL;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
