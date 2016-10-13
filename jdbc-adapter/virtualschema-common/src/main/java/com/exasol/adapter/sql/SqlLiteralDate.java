package com.exasol.adapter.sql;


public class SqlLiteralDate extends SqlNode {

    private String value;   // Stored as YYYY-MM-DD
    
    public SqlLiteralDate(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    @Override
    public String toSimpleSql() {
        return "DATE '" + value + "'";   // This gets always executed as TO_DATE('2015-02-01','YYYY-MM-DD')
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.LITERAL_DATE;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
