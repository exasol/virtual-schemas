package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;

import java.util.ArrayList;
import java.util.List;

/**
 * Node in a graph representing a SQL query.
 */
public abstract class SqlNode {
    
    private SqlNode parent;

    public abstract SqlNodeType getType();


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
    public abstract <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException;

    /**
     * @return A SQL representation of the current graph, using EXASOL SQL syntax. It is called "SIMPLE" because it is not guaranteed to be 100 % correct SQL (e.g. might be ambiguous).
     */
    abstract String toSimpleSql();
}
