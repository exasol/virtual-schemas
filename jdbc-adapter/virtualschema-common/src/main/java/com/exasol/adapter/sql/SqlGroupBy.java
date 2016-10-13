package com.exasol.adapter.sql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

public class SqlGroupBy extends SqlExpressionList {
    
    public SqlGroupBy() {
        
    }
    
    public SqlGroupBy(List<SqlNode> groupByList) {
        super(groupByList);
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.GROUP_BY;
    }
    
    @Override
    public String toSimpleSql() {
        if (getSons().isEmpty()) {
            return "*";
        }
        List<String> selectElement = new ArrayList<>();
        for (SqlNode node : getSons()) {
            selectElement.add(node.toSimpleSql());
        }
        return Joiner.on(", ").join(selectElement);
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
