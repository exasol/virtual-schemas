package com.exasol.adapter.sql;


import com.google.common.collect.ImmutableList;

public class SqlPredicateNotEqual extends SqlPredicate {

    private SqlNode left;
    private SqlNode right;
    
    public SqlPredicateNotEqual(SqlNode left, SqlNode right) {
        super(Predicate.NOTEQUAL);
        this.left = left;
        this.right = right;
        if (this.left != null) {
            this.left.setParent(this);
        }
        if (this.right != null) {
            this.right.setParent(this);
        }
    }
    
    public SqlNode getLeft() {
        return left;
    }
    
    public SqlNode getRight() {
        return right;
    }
    
    @Override
    public String toSimpleSql() {
        return left.toSimpleSql() + " != " + right.toSimpleSql();
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_NOTEQUAL;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
