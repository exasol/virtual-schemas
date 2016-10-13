package com.exasol.adapter.sql;

import java.util.List;

public abstract class SqlExpressionList extends SqlNode {
    
    public SqlExpressionList() {
        
    }
    
    public SqlExpressionList(List<SqlNode> expressions) {
        super(expressions);
        assert(expressions != null);
    }

}
