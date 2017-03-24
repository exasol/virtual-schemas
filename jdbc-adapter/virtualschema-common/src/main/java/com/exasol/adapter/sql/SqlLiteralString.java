package com.exasol.adapter.sql;


import com.exasol.adapter.AdapterException;

public class SqlLiteralString extends SqlNode {

    private String value;
    
    public SqlLiteralString(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    @Override
    public String toSimpleSql() {
        // Don't forget to escape single quote
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.LITERAL_STRING;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
