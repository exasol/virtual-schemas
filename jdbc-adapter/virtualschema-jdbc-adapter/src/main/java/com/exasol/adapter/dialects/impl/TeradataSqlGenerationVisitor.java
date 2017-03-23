package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;


public class TeradataSqlGenerationVisitor extends SqlGenerationVisitor {

    public TeradataSqlGenerationVisitor(SqlDialect dialect, SqlGenerationContext context) {
        super(dialect, context);

    }

    @Override
    public String visit(SqlSelectList selectList) throws AdapterException {
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
    public String visit(SqlStatementSelect select) throws AdapterException {
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
    public String visit(SqlColumn column) throws AdapterException {
        return getColumnProjectionString(column, super.visit(column));
    }

    private String getColumnProjectionString(SqlColumn column, String projString) throws AdapterException {
        boolean isDirectlyInSelectList = (column.hasParent() && column.getParent().getType() == SqlNodeType.SELECT_LIST);
        if (!isDirectlyInSelectList) {
            return projString;
        }
        String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        return getColumnProjectionStringNoCheckImpl(typeName, column, projString);
       
    }

    
    private String getColumnProjectionStringNoCheck(SqlColumn column, String projString) throws AdapterException {
        String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
        return getColumnProjectionStringNoCheckImpl(typeName, column, projString);
        
    }
    

    private String getColumnProjectionStringNoCheckImpl(String typeName, SqlColumn column, String projString) {
    	
    	if ( typeName.startsWith("SYSUDTLIB.ST_GEOMETRY") ||typeName.startsWith("JSON")  ) {
    		
            projString = "CAST(" + projString + "  as VARCHAR("+TeradataSqlDialect.maxTeradataVarcharSize+") )";
            
        }
        else if (typeName.startsWith("XML") ) {
        	
            projString = "XMLSERIALIZE(DOCUMENT " + projString + " as VARCHAR("+TeradataSqlDialect.maxTeradataVarcharSize+") INCLUDING XMLDECLARATION) ";
	
        }
        else if (typeName.startsWith("NUMBER")  &&  column.getMetadata().getType().getExaDataType() == DataType.ExaDataType.DOUBLE  ){
        	
        	projString = "CAST(" + projString + "  as DOUBLE PRECISION)";
        	
        }
        else if (typeName.equals("TIME") || typeName.equals("TIME WITH TIME ZONE") ) {
        	
        	projString = "CAST(" + projString + "  as VARCHAR(21) )";
        	
        }
        else if ( typeName.startsWith("INTERVAL") ) {
        	
        	projString = "CAST(" + projString + "  as VARCHAR(30) )";
        	
        }
        else if ( typeName.startsWith("PERIOD") ) {
        	
        	projString = "CAST(" + projString + "  as VARCHAR(100) )";
        	
        }
        else if ( typeName.startsWith("CLOB") ) {
        	
        	projString = "CAST(" + projString + "  as VARCHAR("+TeradataSqlDialect.maxTeradataVarcharSize+") )";
        	
        }
        else if (TYPE_NAME_NOT_SUPPORTED.contains(typeName)){
        	
        	projString = "'"+typeName+" NOT SUPPORTED'"; //returning a string constant for unsupported data types
        	

        }else if (typeName.startsWith("SYSUDTLIB")){
        	
        	projString = "'"+typeName+" NOT SUPPORTED'"; //returning a string constant for unsupported data types

        }
        	
        return projString;
    }
    
    private static final List<String> TYPE_NAMES_REQUIRING_CAST = ImmutableList.of("SYSUDTLIB.ST_GEOMETRY","XML","JSON","TIME","TIME WITH TIME ZONE","CLOB");
    

    private static final List<String>  TYPE_NAME_NOT_SUPPORTED =  ImmutableList.of("BYTE","VARBYTE","BLOB"); 


    private boolean nodeRequiresCast(SqlNode node) throws AdapterException {
        if (node.getType() == SqlNodeType.COLUMN) {
            SqlColumn column = (SqlColumn)node;
            String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
            return TYPE_NAMES_REQUIRING_CAST.contains(typeName) || 
            		TYPE_NAME_NOT_SUPPORTED.contains(typeName) ||  
            		(typeName.startsWith("NUMBER")  &&  column.getMetadata().getType().getExaDataType() == DataType.ExaDataType.DOUBLE ||
            		typeName.startsWith("INTERVAL")	|| 
            		typeName.startsWith("PERIOD") || 
            		typeName.startsWith("SYSUDTLIB")  //user defined type

            		);
        }
        return false;
    }

    private boolean selectListRequiresCasts(SqlSelectList selectList) throws AdapterException {
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
