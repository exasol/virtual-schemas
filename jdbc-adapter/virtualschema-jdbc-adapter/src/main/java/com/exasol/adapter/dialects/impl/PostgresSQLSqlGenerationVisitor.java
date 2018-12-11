package com.exasol.adapter.dialects.impl;

import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.sql.SqlColumn;
import com.exasol.adapter.sql.SqlFunctionAggregateGroupConcat;
import com.exasol.adapter.sql.SqlFunctionScalar;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlNodeType;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

public class PostgresSQLSqlGenerationVisitor extends SqlGenerationVisitor {

    public PostgresSQLSqlGenerationVisitor(SqlDialect dialect, SqlGenerationContext context) {
        super(dialect, context);
      
    }
    
    @Override
    public String visit(SqlFunctionScalar function) throws AdapterException {
        String sql = super.visit(function);
        
        
		List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        StringBuilder builder = new StringBuilder();
        
        switch (function.getFunction()) {
        
        case ADD_DAYS:
        case ADD_HOURS:
        case ADD_MINUTES:
        case ADD_SECONDS:
        case ADD_WEEKS:
        case ADD_YEARS: { 
        	
            builder.append( argumentsSql.get(0) );
            builder.append(" + ");


            switch (function.getFunction()) {
	            case ADD_DAYS:
	                builder.append(" interval '"+  argumentsSql.get(1) +" day'" );
	                break;
	            case ADD_HOURS:
	                builder.append(" interval '"+  argumentsSql.get(1) +" hour'" );
	                break;
	            case ADD_MINUTES:
	                builder.append(" interval '"+  argumentsSql.get(1) +" minute'" );
	                break;
	            case ADD_SECONDS:
	                builder.append(" interval '"+  argumentsSql.get(1) +" second'" );
	                break;
	            case ADD_WEEKS:
	                builder.append(" interval '"+  argumentsSql.get(1) +" week'" );
	                break;
	            case ADD_YEARS:
	                builder.append(" interval '"+  argumentsSql.get(1) +" year'" );
	                break;
	            default:
	                break;
            }
            
            sql = builder.toString();
            break;
        }
        
        case SECONDS_BETWEEN:
        case MINUTES_BETWEEN:
        case HOURS_BETWEEN:
        case DAYS_BETWEEN:
        case MONTHS_BETWEEN:
        case YEARS_BETWEEN: {
             builder.append("DATE_PART(");
             switch (function.getFunction()) {
 	            case SECONDS_BETWEEN:
 	                builder.append("'SECOND'");
 	                break;
 	            case MINUTES_BETWEEN:
 	                builder.append("'MINUTE'");
 	                break;
 	            case HOURS_BETWEEN:
 	                builder.append("'HOUR'");
 	                break;
 	            case DAYS_BETWEEN:
 	                builder.append("'DAY'");
 	                break;
 	            case MONTHS_BETWEEN:
 	                builder.append("'MONTH'");
 	                break;
 	            case YEARS_BETWEEN:
 	                builder.append("'YEAR'");
 	                break;
 	            default:
 	                break;
             }
             builder.append(", AGE(");
             builder.append( argumentsSql.get(1) );
             builder.append(",");
             builder.append( argumentsSql.get(0) );
             builder.append("))");
             sql = builder.toString();
             break;
        }
        
        case SECOND:
        case MINUTE:
        case DAY:
        case WEEK:
        case MONTH:
        case YEAR: {
        	builder.append("DATE_PART(");
            switch (function.getFunction()) {
	            case SECOND:
	            	builder.append("'SECOND'");
	 	            break;
	            case MINUTE:
	            	builder.append("'MINUTE'");
 	                break;
	            case DAY:
	            	builder.append("'DAY'");
 	                break;
	            case WEEK:
	            	builder.append("'WEEK'");
 	                break;
	            case MONTH:
	            	builder.append("'MONTH'");
 	                break;
	            case YEAR:
	            	builder.append("'YEAR'");
 	                break;
	            default:
 	                break;
            }
            builder.append(",");
            builder.append( argumentsSql.get(0) );
            builder.append(")");
            sql = builder.toString();
            break;    
        } 
        case POSIX_TIME: {
        	
        	builder.append("EXTRACT(EPOCH FROM ");
            builder.append( argumentsSql.get(0) );
            builder.append(")");
            sql = builder.toString();
 
        }
        	
        break;
		default:
			break;
        
        
        }
        return sql;
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
    	
    	if (
	    		typeName.startsWith("varbit") ||  
	    		typeName.startsWith("point") ||
	    		typeName.startsWith("line") || 
	    		typeName.startsWith("lseg") || 
	    		typeName.startsWith("box") || 
	    		typeName.startsWith("path") || 
	    		typeName.startsWith("polygon") || 
	    		typeName.startsWith("circle")  ||
                typeName.startsWith("cidr") ||
                typeName.startsWith("citext") ||
	    		typeName.startsWith("inet") ||
	    		typeName.startsWith("macaddr") ||
	    		typeName.startsWith("interval") ||
	    		typeName.startsWith("json") ||
	    		typeName.startsWith("jsonb") ||
	    		typeName.startsWith("uuid") ||
	    		typeName.startsWith("tsquery") ||
	    		typeName.startsWith("tsvector") ||
	    		typeName.startsWith("xml") 

    		) {
    		
            //projString = "CAST(" + projString + "  as VARCHAR("+PostgreSQLSqlDialect.maxPostgresSQLVarcharSize+") )";
            projString = "CAST(" + projString + "  as VARCHAR )";

        }
        else if (TYPE_NAME_NOT_SUPPORTED.contains(typeName)){
        	
        	projString = "cast('"+typeName+" NOT SUPPORTED' as varchar) as not_supported"; //returning a string constant for unsupported data types
        	
        }
        	
        return projString;
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
    
    private static final List<String> TYPE_NAMES_REQUIRING_CAST = ImmutableList.of("varbit","point","line","lseg","box","path","polygon","circle","cidr","citext","inet","macaddr","interval","json","jsonb","uuid","tsquery", "tsvector","xml");
    
    private static final List<String>  TYPE_NAME_NOT_SUPPORTED =  ImmutableList.of("bytea"); 


    private boolean nodeRequiresCast(SqlNode node) throws AdapterException {
        if (node.getType() == SqlNodeType.COLUMN) {
            SqlColumn column = (SqlColumn)node;
            String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
            return TYPE_NAMES_REQUIRING_CAST.contains(typeName) || 
            		TYPE_NAME_NOT_SUPPORTED.contains(typeName) 
            		;
        }
        return false;
    }

    
    @Override
    public String visit(SqlFunctionAggregateGroupConcat function) throws AdapterException {
        StringBuilder builder = new StringBuilder();
        builder.append("STRING_AGG");
        builder.append("(");
        assert(function.getArguments() != null);
        assert(function.getArguments().size() == 1 && function.getArguments().get(0) != null);
        String expression = function.getArguments().get(0).accept(this);
        builder.append(expression);
        builder.append(", ");
        String separator = ",";
        if (function.getSeparator() != null) {
            separator = function.getSeparator();
        }
        builder.append("'");
        builder.append(separator);
        builder.append("') ");

        return builder.toString();
    }

   
}