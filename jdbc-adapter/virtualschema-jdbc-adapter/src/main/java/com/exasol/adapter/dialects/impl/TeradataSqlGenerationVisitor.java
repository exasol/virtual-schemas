package com.exasol.adapter.dialects.impl;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.sql.SqlColumn;
import com.exasol.adapter.sql.SqlLimit;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlNodeType;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;


public class TeradataSqlGenerationVisitor extends SqlGenerationVisitor {

    public TeradataSqlGenerationVisitor(SqlDialect dialect, SqlGenerationContext context) {
        super(dialect, context);

    }

    @Override
    public String visit(SqlSelectList selectList) {
        if (selectList.isRequestAnyColumn()) {
            // The system requested any column
            return "1";
        }
        List<String> selectListElements = new ArrayList<>();
        if (selectList.isSelectStar()) {
            if (selectListRequiresCasts(selectList)) {
                // Do as if the user has all columns in select list
                SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
                
                int columnId = 0;
                for (ColumnMetadata columnMeta : select.getFromClause().getMetadata().getColumns()) {
                    SqlColumn sqlColumn = new SqlColumn(columnId, columnMeta);
                    selectListElements.add( getColumnProjectionStringNoCheck(sqlColumn,  super.visit(sqlColumn)  )   );
                    ++columnId;
                }
                
            } else {
                selectListElements.add("*");
            }
        } else {
            for (SqlNode node : selectList.getExpressions()) {
                selectListElements.add(node.accept(this));
            }
        }
       
        return Joiner.on(", ").join(selectListElements);
    }
    
    
    @Override
    public String visit(SqlStatementSelect select) {
        if (!select.hasLimit()) {
            return super.visit(select);
        } else {
            SqlLimit limit = select.getLimit();
         
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT TOP "+limit.getLimit()+ " ");
            
            sql.append(select.getSelectList().accept(this));
            sql.append(" FROM ");
            sql.append(select.getFromClause().accept(this));
            if (select.hasFilter()) {
                sql.append(" WHERE ");
                sql.append(select.getWhereClause().accept(this));
            }
            if (select.hasGroupBy()) {
                sql.append(" GROUP BY ");
                sql.append(select.getGroupBy().accept(this));
            }
            if (select.hasHaving()) {
                sql.append(" HAVING ");
                sql.append(select.getHaving().accept(this));
            }
            if (select.hasOrderBy()) {
                sql.append(" ");
                sql.append(select.getOrderBy().accept(this));
            }

            return sql.toString();   
        }
    }
    
    
    @Override
    public String visit(SqlColumn column) {
        return getColumnProjectionString(column, super.visit(column));
    }

    private String getColumnProjectionString(SqlColumn column, String projString) {
        boolean isDirectlyInSelectList = (column.hasParent() && column.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (!isDirectlyInSelectList) {
            return projString;
        }
       
        String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        
        return getColumnProjectionStringNoCheckImpl(typeName, projString);
       
    }

    
    private String getColumnProjectionStringNoCheck(SqlColumn column, String projString) {

        String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        
        return getColumnProjectionStringNoCheckImpl(typeName, projString);
        
    }
    
    
    private String getColumnProjectionStringNoCheckImpl(String typeName,String projString) {
    	if ( typeName.startsWith("SYSUDTLIB.ST_GEOMETRY") ||typeName.startsWith("JSON")  )
        {
        	
            projString = "CAST(" + projString + "  as VARCHAR(64000) )";
            
        } 
        
        else if (typeName.startsWith("XML") ) {
        	
            projString = "XMLSERIALIZE(DOCUMENT " + projString + " as VARCHAR(64000) INCLUDING XMLDECLARATION) ";
	
        }         
        	
        return projString;
    }
    
    
    private static final List<String> TYPE_NAMES_REQUIRING_CAST = ImmutableList.of("SYSUDTLIB.ST_GEOMETRY","XML");

    private boolean nodeRequiresCast(SqlNode node) {
        if (node.getType() == SqlNodeType.COLUMN) {
            SqlColumn column = (SqlColumn)node;
            String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
            return TYPE_NAMES_REQUIRING_CAST.contains(typeName);
        }
        return false;
    }

    private boolean selectListRequiresCasts(SqlSelectList selectList) {
        boolean requiresCasts = false;

        // Do as if the user has all columns in select list
        SqlStatementSelect select = (SqlStatementSelect) selectList.getParent();
        int columnId = 0;
        for (ColumnMetadata columnMeta : select.getFromClause().getMetadata().getColumns()) {
        	if (nodeRequiresCast(new SqlColumn(columnId, columnMeta))) {
                requiresCasts = true;
                        	}
        }

        return requiresCasts;
    }
}
