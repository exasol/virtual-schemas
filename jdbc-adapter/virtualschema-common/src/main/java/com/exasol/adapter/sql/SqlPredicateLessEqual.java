package com.exasol.adapter.sql;


import com.google.common.collect.ImmutableList;

public class SqlPredicateLessEqual extends SqlPredicate {

    private SqlNode left;
    private SqlNode right;
    
    public SqlPredicateLessEqual(SqlNode left, SqlNode right) {
        super(ImmutableList.of(left, right), Predicate.LESSEQUAL);
        this.left = left;
        this.right = right;
    }
    
    public SqlNode getLeft() {
        return left;
    }
    
    public SqlNode getRight() {
        return right;
    }
    
    @Override
    public String toSimpleSql() {
        return left.toSimpleSql() + " <= " + right.toSimpleSql();
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_LESSEQUAL;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
