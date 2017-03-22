package com.exasol.adapter.sql;

import com.exasol.adapter.AdapterException;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

public class SqlSelectList extends SqlExpressionList {

    SqlSelectListType type;

    private SqlSelectList(SqlSelectListType type, List<SqlNode> selectList) {
        super(selectList);
        this.type = type;
    }

    /**
     * Creates a SqlSelectList for SELECT *. See {@link SqlSelectListType#SelectStar}.
     * @return the new SqlSelectList.
     */
    public static SqlSelectList createSelectStarSelectList() {
        return new SqlSelectList(SqlSelectListType.SelectStar, null);
    }

    /**
     * Creates a SqlSelectList that uses an arbitrary value. See {@link SqlSelectListType#AnyValue}.
     * @return the new SqlSelectList.
     */
    public static SqlSelectList createAnyValueSelectList() {
        return new SqlSelectList(SqlSelectListType.AnyValue, null);
    }

    /**
     * Creates a regular SqlSelectList. See {@link SqlSelectListType#Regular}.
     * @param selectList The selectList needs at least one element.
     * @return the new SqlSelectList.
     */
    public static SqlSelectList createRegularSelectList(List<SqlNode> selectList) {
        assert (selectList != null);
        assert (selectList.size() > 0);
        return new SqlSelectList(SqlSelectListType.Regular, selectList);
    }



    public boolean isRequestAnyColumn() {
        return type == SqlSelectListType.AnyValue;
    }

    /**
     * @return true if this is "SELECT *", false otherwise
     */
    public boolean isSelectStar() {
        return type == SqlSelectListType.SelectStar;
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.SELECT_LIST;
    }

    @Override
    public String toSimpleSql() {
        if (isRequestAnyColumn()) {
            // The system requested any column
            return "true";
        }
        if (isSelectStar()) {
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
