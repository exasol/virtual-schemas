package com.exasol.adapter.sql;


import com.exasol.adapter.AdapterException;

public class SqlPredicateBetween extends SqlPredicate {

    private SqlNode expression;
    private SqlNode betweenLeft;
    private SqlNode betweenRight;
    
    public SqlPredicateBetween(SqlNode expression, SqlNode betweenLeft, SqlNode betweenRight) {
        super(Predicate.BETWEEN);
        this.expression = expression;
        this.betweenLeft = betweenLeft;
        this.betweenRight = betweenRight;
        if (this.expression != null) {
            this.expression.setParent(this);
        }
        if (this.betweenLeft != null) {
            this.betweenLeft.setParent(this);
        }
        if (this.betweenRight != null) {
            this.betweenRight.setParent(this);
        }
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
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
