package com.exasol.adapter.sql;


import com.google.common.collect.ImmutableList;

public class SqlPredicateEqual extends SqlPredicate {

    private SqlNode left;
    private SqlNode right;
    
    public SqlPredicateEqual(SqlNode left, SqlNode right) {
        super(ImmutableList.of(left, right), Predicate.EQUAL);
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
        return left.toSimpleSql() + " = " + right.toSimpleSql();
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_EQUAL;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
