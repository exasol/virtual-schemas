package com.exasol.adapter.sql;


import com.exasol.adapter.AdapterException;

public class SqlLiteralDouble extends SqlNode {

    private double value;
    
    public SqlLiteralDouble(double value) {
        this.value = value;
    }
    
    public double getValue() {
        return value;
    }
    
    @Override
    public String toSimpleSql() {
        return Double.toString(value);
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.LITERAL_DOUBLE;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
