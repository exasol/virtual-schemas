package com.exasol.adapter.sql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

public class SqlSelectList extends SqlExpressionList {

    /**
     * If true, we just need one arbitrary value for each row. Example: If user
     * runs COUNT (*) and COUNT cannot be pushed down, we need to return any
     * value for each row (e.g. constant TRUE) and then EXASOL can do the COUNT.
     */
    boolean requestedAnyColumn = false;

    /**
     * Call this if all columns are required, i.e. SELECT * FROM ...
     */
    public SqlSelectList() {super(null);}

    /**
     * This is required in two cases<br>
     * 1. If selectList has > 1 elements: This is a regular select list 2. If
     * selectList has no element: This means that any column is required
     */
    public SqlSelectList(List<SqlNode> selectList) {
        super(selectList);
        if (selectList.size() == 0) {
            requestedAnyColumn = true;
        }
    }

    public boolean isRequestAnyColumn() {
        return requestedAnyColumn;
    }

    /**
     * @return true if this is "SELECT *", false otherwise
     */
    public boolean isSelectStar() {
        return getExpressions() == null || getExpressions().isEmpty();
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.SELECT_LIST;
    }

    @Override
    public String toSimpleSql() {
        if (requestedAnyColumn) {
            // The system requested any column
            return "true";
        }
        if (getExpressions() == null || getExpressions().isEmpty()) {
            return "*";
        }
        List<String> selectElement = new ArrayList<>();
        for (SqlNode node : getExpressions()) {
            selectElement.add(node.toSimpleSql());
        }
        return Joiner.on(", ").join(selectElement);
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
