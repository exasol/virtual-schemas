package com.exasol.adapter.sql;


import com.exasol.adapter.AdapterException;

public class SqlPredicateIsNotNull extends SqlPredicate {

    private SqlNode expression;

    public SqlPredicateIsNotNull(SqlNode expression) {
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
        return expression.toSimpleSql() + " IS NOT NULL";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_IS_NULL;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
