package com.exasol.adapter.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * Node in a graph representing a SQL query.
 */
public abstract class SqlNode {
    
    private List<SqlNode> sons;
    private SqlNode parent;

    public SqlNode() {
        this.sons = new ArrayList<>();
    }
    
    public SqlNode(List<SqlNode> sons) {
        this.sons = sons;
        // set parents
        for (SqlNode son : sons) {
            son.setParent(this);
        }
    }
    
    public abstract SqlNodeType getType();
    
    public SqlNode getSon(int i) {
        return sons.get(i);
    }
    
    public List<SqlNode> getSons() {
        return sons;
    }
    
    public void setSons(List<SqlNode> sons) {
        this.sons = sons;
        // set parents
        for (SqlNode son : sons) {
            son.setParent(this);
        }
    }

    public void setParent(SqlNode parent) {
        this.parent = parent;
    }

    public SqlNode getParent() {
        return parent;
    }

    public boolean hasParent() {
        return (this.parent != null);
    }
    
    /**
     * See {@link SqlNodeVisitor}
     * @param visitor The visitor object on which the appropriate visit(sqlNode) method is called
     */
    public abstract <R> R accept(SqlNodeVisitor<R> visitor);

    /**
     * @return A SQL representation of the current graph, using EXASOL SQL syntax. It is called "SIMPLE" because it is not guaranteed to be 100 % correct SQL (e.g. might be ambiguous).
     */
    abstract String toSimpleSql();
}
