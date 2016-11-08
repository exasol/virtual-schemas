package com.exasol.adapter.sql;


import com.google.common.collect.ImmutableList;

public class SqlPredicateNot extends SqlPredicate {

    private SqlNode expression;
    
    public SqlPredicateNot(SqlNode expression) {
        super(Predicate.NOT);
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
        return "NOT (" + expression.toSimpleSql() + ")";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_NOT;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
