package com.exasol.adapter.sql;


import com.exasol.adapter.AdapterException;

public class SqlPredicateLikeRegexp extends SqlPredicate {

    private SqlNode left;
    private SqlNode pattern;
    
    public SqlPredicateLikeRegexp(SqlNode left, SqlNode pattern) {
        super(Predicate.REGEXP_LIKE);
        this.left = left;
        this.pattern = pattern;
        if (this.left != null) {
            this.left.setParent(this);
        }
        if (this.pattern != null) {
            this.pattern.setParent(this);
        }
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
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
