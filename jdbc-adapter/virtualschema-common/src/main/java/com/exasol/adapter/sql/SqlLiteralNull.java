package com.exasol.adapter.sql;


import com.exasol.adapter.AdapterException;

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
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
