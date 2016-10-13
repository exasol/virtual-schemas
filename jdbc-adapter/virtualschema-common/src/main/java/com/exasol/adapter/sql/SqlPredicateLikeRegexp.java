package com.exasol.adapter.sql;


import com.google.common.collect.ImmutableList;

public class SqlPredicateLikeRegexp extends SqlPredicate {

    private SqlNode left;
    private SqlNode pattern;
    
    public SqlPredicateLikeRegexp(SqlNode left, SqlNode pattern) {
        super(ImmutableList.of(left, pattern), Predicate.REGEXP_LIKE);
        this.left = left;
        this.pattern = pattern;
    }
    
    public SqlNode getLeft() {
        return left;
    }
    
    public SqlNode getPattern() {
        return pattern;
    }
    
    @Override
    public String toSimpleSql() {
        return left.toSimpleSql() + " REGEXP_LIKE " + pattern.toSimpleSql();
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_LIKE_REGEXP;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
