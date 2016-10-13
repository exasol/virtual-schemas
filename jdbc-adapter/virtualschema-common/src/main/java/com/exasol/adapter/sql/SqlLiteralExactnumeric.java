package com.exasol.adapter.sql;

import java.math.BigDecimal;

public class SqlLiteralExactnumeric extends SqlNode {

    private BigDecimal value;
    
    public SqlLiteralExactnumeric(BigDecimal value) {
        this.value = value;
    }
    
    public BigDecimal getValue() {
        return value;
    }
    
    @Override
    public String toSimpleSql() {
        return value.toString();
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.LITERAL_EXACTNUMERIC;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
