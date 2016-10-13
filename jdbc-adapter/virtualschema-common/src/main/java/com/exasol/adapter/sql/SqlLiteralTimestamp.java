package com.exasol.adapter.sql;


public class SqlLiteralTimestamp extends SqlNode {

    private String value;   // stored as YYYY-MM-DD HH:MI:SS.FF6
    
    public SqlLiteralTimestamp(String value) {
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
        return SqlNodeType.LITERAL_TIMESTAMP;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
