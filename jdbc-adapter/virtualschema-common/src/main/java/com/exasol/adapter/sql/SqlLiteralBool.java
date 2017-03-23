package com.exasol.adapter.sql;


import com.exasol.adapter.AdapterException;

public class SqlLiteralBool extends SqlNode {

    private boolean value;
    
    public SqlLiteralBool(boolean value) {
        this.value = value;
    }
    
    public boolean getValue() {
        return value;
    }
    
    @Override
    public String toSimpleSql() {
        if (value) {
            return "true";
        } else {
            return "false";
        }
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.LITERAL_BOOL;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
