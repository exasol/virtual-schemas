package com.exasol.adapter.sql;

import java.util.Collections;
import java.util.List;

public abstract class SqlExpressionList extends SqlNode {

    private List<SqlNode> expressions;
    
    public SqlExpressionList(List<SqlNode> expressions) {
        this.expressions = expressions;
        if (this.expressions != null) {
            for (SqlNode node : this.expressions) {
                node.setParent(this);
            }
        }
    }

    public List<SqlNode> getExpressions() {
        if (expressions == null) {
            return null;
        } else {
            return Collections.unmodifiableList(expressions);
        }
    }

}
