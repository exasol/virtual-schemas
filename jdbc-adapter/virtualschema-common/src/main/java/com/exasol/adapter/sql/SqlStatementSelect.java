package com.exasol.adapter.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * We could consider to apply builder pattern here (if time)
 */
public class SqlStatementSelect extends SqlStatement {

    private SqlTable fromClause;        // can be changed to SqlNode later if we support more complex things
    private SqlSelectList selectList;
    private SqlNode whereClause;
    private SqlExpressionList groupBy;
    private SqlNode having;
    private SqlOrderBy orderBy;
    private SqlLimit limit;
    
    public SqlStatementSelect(SqlTable fromClause, SqlSelectList selectList, SqlNode whereClause, SqlExpressionList groupBy, SqlNode having, SqlOrderBy orderBy, SqlLimit limit) {
        this.fromClause = fromClause;
        this.selectList = selectList;
        this.whereClause = whereClause;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.limit = limit;
        assert(this.fromClause != null);
        assert(this.selectList != null);
        this.fromClause.setParent(this);
        this.selectList.setParent(this);

        if (this.whereClause != null) {
            this.whereClause.setParent(this);
        }
        if (this.groupBy != null) {
            this.groupBy.setParent(this);
        }
        if (this.having != null) {
            this.having.setParent(this);
        }
        if (this.orderBy != null) {
            this.orderBy.setParent(this);
        }
        if (this.limit != null) {
            this.limit.setParent(this);
        }
    }
    
    public boolean hasProjection() {
        return selectList != null;
    }
    
    public boolean hasGroupBy() {
        return groupBy != null;
    }
    
    public boolean hasHaving() {
        return having != null;
    }
    
    public boolean hasFilter() {
        return whereClause != null;
    }
    
    public boolean hasOrderBy() {
        return orderBy != null;
    }
    
    public boolean hasLimit() {
        return limit != null;
    }
    
    public SqlTable getFromClause() {
        return fromClause;
    }
    
    public SqlSelectList getSelectList() {
        return selectList;
    }
    
    public SqlNode getWhereClause() {
        return whereClause;
    }
    
    public SqlExpressionList getGroupBy() {
        return groupBy;
    }
    
    public SqlNode getHaving() {
        return having;
    }

    public SqlOrderBy getOrderBy() {
        return orderBy;
    }
    
    public SqlLimit getLimit() {
        return limit;
    }
    
    @Override
    public String toSimpleSql() {
        
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        sql.append(selectList.toSimpleSql());
        sql.append(" FROM ");
        sql.append(fromClause.toSimpleSql());
        if (hasFilter()) {
            sql.append(" WHERE " + whereClause.toSimpleSql());
        }
        if (hasGroupBy()) {
            sql.append(" GROUP BY " + groupBy.toSimpleSql());
        }
        if (hasHaving()) {
            sql.append(" HAVING " + having.toSimpleSql());
        }
        if (hasOrderBy()) {
            sql.append(" " + orderBy.toSimpleSql());
        }
        if (hasLimit()) {
            sql.append(" " + limit.toSimpleSql());
        }
        return sql.toString();
    }

    @Override
    public SqlNodeType getType() {
        return SqlNodeType.SELECT;
    }

    @Override
    public <R> R accept(SqlNodeVisitor<R> visitor) {
        return visitor.visit(this);
    }

}
