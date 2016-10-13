package com.exasol.adapter.sql;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

public class SqlOrderBy extends SqlNode {
    
    List<SqlNode> expressions;
    List<Boolean> isAsc;

    // True, if the desired position of nulls is at the end, false if at beginning.
    // This does not necessarily mean the user explicitly specified NULLS LAST or NULLS FIRST.
    List<Boolean> nullsLast;
    
    public SqlOrderBy(List<SqlNode> expressions, List<Boolean> isAsc, List<Boolean> nullsFirst) {
        super(expressions);
        this.expressions = expressions;
        this.isAsc = isAsc;
        this.nullsLast = nullsFirst;
    }
    
    public List<SqlNode> getExpressions() {
        return expressions;
    }
    
    public List<Boolean> isAscending() {
        return isAsc;
    }
    
    public List<Boolean> nullsLast() {
        return nullsLast;
    }
    
    @Override
    public String toSimpleSql() {
        // ORDER BY <expr> [ASC/DESC] [NULLS FIRST/LAST]
        // ASC and NULLS LAST are default
        List<String> sqlOrderElement = new ArrayList<>();
        for (int i=0; i<getSons().size(); ++i) {
            String elementSql = getSon(i).toSimpleSql();
            if (isAsc.get(i) == false) {
                elementSql += " DESC";
            }
            if (nullsLast.get(i) == false) {
                elementSql += " NULLS FIRST";
            }
            sqlOrderElement.add(elementSql);
        }
        return "ORDER BY " + Joiner.on(", ").join(sqlOrderElement);
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.ORDER_BY;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
