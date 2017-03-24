package com.exasol.adapter.sql;


import com.exasol.adapter.AdapterException;

public class SqlLiteralTimestampUtc extends SqlNode {

    private String value;   // stored as YYYY-MM-DD HH:MI:SS.FF6
    
    public SqlLiteralTimestampUtc(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    @Override
    public String toSimpleSql() {
        return "TIMESTAMP '" + value.toString() + "'";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.LITERAL_TIMESTAMPUTC;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
