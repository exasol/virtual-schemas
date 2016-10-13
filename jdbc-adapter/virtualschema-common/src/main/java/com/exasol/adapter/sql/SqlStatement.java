package com.exasol.adapter.sql;

import java.util.List;

public abstract class SqlStatement extends SqlNode {
    
    public SqlStatement(List<SqlNode> sons) {
        super(sons);
    }
    
    public SqlStatement() {
        
    }
    
}
