package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

public class SqlGroupBy extends SqlExpressionList {

    public SqlGroupBy(List<SqlNode> groupByList) {
        super(groupByList);
        if (this.getExpressions() != null) {
            for (SqlNode node : this.getExpressions()) {
                node.setParent(this);
            }
        }
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.GROUP_BY;
    }
    
    @Override
    public String toSimpleSql() {
        if (getExpressions().isEmpty()) {
            return "*";
        }
        List<String> selectElement = new ArrayList<>();
        for (SqlNode node : getExpressions()) {
            selectElement.add(node.toSimpleSql());
        }
        return Joiner.on(", ").join(selectElement);
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) throws AdapterException {
        return visitor.visit(this);
    }

}
