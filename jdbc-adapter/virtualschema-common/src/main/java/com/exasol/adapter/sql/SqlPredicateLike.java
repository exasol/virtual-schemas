package com.exasol.adapter.sql;


import com.exasol.adapter.AdapterException;

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
        super(Predicate.LIKE);
        this.left = left;
        this.pattern = pattern;
        this.escapeChar = escapeChar;
        if (this.left != null) {
            this.left.setParent(this);
        }
        if (this.pattern != null) {
            this.pattern.setParent(this);
        }
        if (this.escapeChar != null) {
            this.escapeChar.setParent(this);
        }
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
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
