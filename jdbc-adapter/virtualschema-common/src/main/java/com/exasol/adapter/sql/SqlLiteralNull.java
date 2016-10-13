package com.exasol.adapter.sql;


public class SqlLiteralNull extends SqlNode {
    
    @Override
    public String toSimpleSql() {
        return "NULL";
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.LITERAL_NULL;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
