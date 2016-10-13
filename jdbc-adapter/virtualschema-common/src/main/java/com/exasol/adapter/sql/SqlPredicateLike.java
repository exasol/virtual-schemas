package com.exasol.adapter.sql;


import com.google.common.collect.ImmutableList;

/**
 *
 */
public class SqlPredicateLike extends SqlPredicate {

    private SqlNode left;
    private SqlNode pattern;
    private SqlNode escapeChar;

    public SqlPredicateLike(SqlNode left, SqlNode pattern) {
        this(left, pattern, null);
    }

    public SqlPredicateLike(SqlNode left, SqlNode pattern, SqlNode escapeChar) {
        super(ImmutableList.of(left, pattern), Predicate.LIKE);
        this.left = left;
        this.pattern = pattern;
        this.escapeChar = escapeChar;
    }
    
    public SqlNode getLeft() {
        return left;
    }
    
    public SqlNode getPattern() {
        return pattern;
    }

    public SqlNode getEscapeChar() {
        return escapeChar;
    }
    
    @Override
    public String toSimpleSql() {
        String sql = left.toSimpleSql() + " LIKE " + pattern.toSimpleSql();
        if (escapeChar != null) {
            sql += " ESCAPE " + escapeChar;
        }
        return sql;
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.PREDICATE_LIKE;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
