package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.jdbc.ColumnAdapterNotes;
import com.exasol.adapter.metadata.ColumnMetadata;
import com.exasol.adapter.sql.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;


public class SqlServerSqlGenerationVisitor extends SqlGenerationVisitor {

    public SqlServerSqlGenerationVisitor(SqlDialect dialect, SqlGenerationContext context) {
        super(dialect, context);

    }

    @Override
    public String visit(SqlSelectList selectList) throws AdapterException {
        if (selectList.isRequestAnyColumn()) {
            // The system requested any column
            return "true";
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
    	
    	if ( typeName.startsWith("text") ) {
            projString = "CAST(" + projString + "  as NVARCHAR("+SqlServerSqlDialect.maxSqlServerNVarcharSize+") )";
        } else if ( typeName.startsWith("date") || typeName.startsWith("datetime2") ) {
            projString = "CAST(" + projString + "  as DateTime )";
        } else if (typeName.startsWith("hierarchyid") ) {
            projString = "CAST(" + projString + "  as NVARCHAR("+SqlServerSqlDialect.maxSqlServerNVarcharSize+") )";
        } else if (typeName.startsWith("geometry") || typeName.startsWith("geography") ) {
            projString = "CAST(" + projString + "  as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )";
        } else if (typeName.startsWith("timestamp") ){
            projString = "CAST(" + projString + "  as DateTime )"; 
        } else if (typeName.startsWith("xml")) {
            projString = "CAST(" + projString + "  as NVARCHAR("+SqlServerSqlDialect.maxSqlServerNVarcharSize+") )";
        } else if (TYPE_NAME_NOT_SUPPORTED.contains(typeName)){
        	projString = "'"+typeName+" NOT SUPPORTED'"; //returning a string constant for unsupported data types
        } 
        	
        return projString;
    }
    
    private static final List<String> TYPE_NAMES_REQUIRING_CAST = 
    		ImmutableList.of("text", "date", "datetime2","hierarchyid","geometry","geography", "timestamp","xml");
    
    private static final List<String>  TYPE_NAME_NOT_SUPPORTED =  ImmutableList.of("varbinary","binary"); 

    private boolean nodeRequiresCast(SqlNode node) throws AdapterException {
        if (node.getType() == SqlNodeType.COLUMN) {
            SqlColumn column = (SqlColumn)node;
			String typeName = ColumnAdapterNotes.deserialize(column.getMetadata().getAdapterNotes(), column.getMetadata().getName()).getTypeName();
			return TYPE_NAMES_REQUIRING_CAST.contains(typeName) || TYPE_NAME_NOT_SUPPORTED.contains(typeName) ;
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
    
    
    @Override
    public String visit(SqlFunctionScalar function) throws AdapterException {

        String sql = super.visit(function);
		List<String> argumentsSql = new ArrayList<>();
        for (SqlNode node : function.getArguments()) {
            argumentsSql.add(node.accept(this));
        }
        StringBuilder builder = new StringBuilder();
        
        switch (function.getFunction()) {
        case INSTR: {

            builder.append("CHARINDEX(");
            builder.append(argumentsSql.get(1));
            builder.append(", ");
            builder.append(argumentsSql.get(0));
            if (argumentsSql.size() > 2) {
                builder.append(", ");
                builder.append(argumentsSql.get(2));
            }
            builder.append(")");
            sql = builder.toString();
            break;
        }
        
        
        
        case LPAD: {  //RIGHT(REPLICATE(pad_char, length) + LEFT(string, length), length)

            String padChar = "' '";
            
            if (argumentsSql.size() > 2) {
            	padChar = argumentsSql.get(2);
            }
            

            String string = argumentsSql.get(0);
            
            String length = argumentsSql.get(1);

            
            builder.append("RIGHT ( REPLICATE(");
            builder.append(padChar);
            builder.append(",");
            builder.append(length);         
            builder.append(") + LEFT(");
            builder.append(string);
            builder.append(",");
            builder.append(length);         
            builder.append("),");
            builder.append(length);         
            builder.append(")");
            sql = builder.toString();
            break;
        }
        
        
        case RPAD: {   //LEFT(RIGHT(string, length) + REPLICATE(pad_char, length) , length);

            String padChar = "' '";
            
            if (argumentsSql.size() > 2) {
            	padChar = argumentsSql.get(2);
            }
            
            String string = argumentsSql.get(0);
            
            String length = argumentsSql.get(1);

            builder.append("LEFT(RIGHT(");
            builder.append(string);
            builder.append(",");
            builder.append(length);
            builder.append(") + REPLICATE(");
            builder.append(padChar);
            builder.append(",");
            builder.append(length);      
            builder.append("),");
            builder.append(length);         
            builder.append(")");
            sql = builder.toString();
            break;
        
        }
        case ADD_DAYS:
        case ADD_HOURS:
        case ADD_MINUTES:
        case ADD_SECONDS:
        case ADD_WEEKS:
        case ADD_YEARS: { //DATEADD(datepart,number,date)

            builder.append("DATEADD(");

            switch (function.getFunction()) {
	            case ADD_DAYS:
	                builder.append("DAY");
	                break;
	            case ADD_HOURS:
	                builder.append("HOUR");
	                break;
	            case ADD_MINUTES:
	                builder.append("MINUTE");
	                break;
	            case ADD_SECONDS:
	                builder.append("SECOND");
	                break;
	            case ADD_WEEKS:
	                builder.append("WEEK");
	                break;
	            case ADD_YEARS:
	                builder.append("YEAR");
	                break;
	            default:
	                break;
            }
            
            builder.append(",");
            builder.append( argumentsSql.get(1) );
            builder.append(",");
            builder.append( argumentsSql.get(0) );
            builder.append(")");
            sql = builder.toString();
            break;
        }
        case SECONDS_BETWEEN:
        case MINUTES_BETWEEN:
        case HOURS_BETWEEN:
        case DAYS_BETWEEN:
        case MONTHS_BETWEEN:
        case YEARS_BETWEEN: {

             builder.append("DATEDIFF(");

             switch (function.getFunction()) {
 	            case SECONDS_BETWEEN:
 	                builder.append("SECOND");
 	                break;
 	            case MINUTES_BETWEEN:
 	                builder.append("MINUTE");
 	                break;
 	            case HOURS_BETWEEN:
 	                builder.append("HOUR");
 	                break;
 	            case DAYS_BETWEEN:
 	                builder.append("DAY");
 	                break;
 	            case MONTHS_BETWEEN:
 	                builder.append("MONTH");
 	                break;
 	            case YEARS_BETWEEN:
 	                builder.append("YEAR");
 	                break;
 	            default:
 	                break;
             }
             
             builder.append(",");
             builder.append( argumentsSql.get(1) );
             builder.append(",");
             builder.append( argumentsSql.get(0) );
             builder.append(")");
             sql = builder.toString();
             break;
        }
        case CURRENT_DATE:
            sql = "CAST( GETDATE() AS DATE)";
            break;
            
        case CURRENT_TIMESTAMP:
            sql = "GETDATE()";
            break;

        case SYSDATE:
            sql = "CAST( SYSDATETIME() AS DATE)";
            break;
            
        case SYSTIMESTAMP:
            sql = "SYSDATETIME()";
            break;
            
 
        case ST_X:
	    		builder.append(argumentsSql.get(0)+".STX") ;
	    		sql = builder.toString();
	    		break;

	    case ST_Y:
	    		builder.append(argumentsSql.get(0)+".STY") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_ENDPOINT:
            	builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STEndPoint()") ;
            	builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_ISCLOSED:
	    		builder.append(argumentsSql.get(0)+".STIsClosed()") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_ISRING:
	    		builder.append(argumentsSql.get(0)+".STIsRing()") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_LENGTH:
	    		builder.append(argumentsSql.get(0)+".STLength()") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_NUMPOINTS:
	    		builder.append(argumentsSql.get(0)+".STNumPoints()") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_POINTN:
        		builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STPointN("+argumentsSql.get(1)+")") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_STARTPOINT:
    			builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STStartPoint()") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_AREA:
	    		builder.append(argumentsSql.get(0)+".STArea()") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_EXTERIORRING:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STExteriorRing()") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_INTERIORRINGN:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STInteriorRingN ("+argumentsSql.get(1)+")") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_NUMINTERIORRINGS:
	    		builder.append(argumentsSql.get(0)+".STNumInteriorRing()") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_GEOMETRYN:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STGeometryN("+argumentsSql.get(1)+")") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_NUMGEOMETRIES:
	    		builder.append(argumentsSql.get(0)+".STNumGeometries()") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_BOUNDARY:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STBoundary()") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_BUFFER:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STBuffer("+argumentsSql.get(1)+")") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_CENTROID:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STCentroid()") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_CONTAINS:
	    		builder.append(argumentsSql.get(0)+".STContains("+argumentsSql.get(1)+")") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_CONVEXHULL:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STConvexHull()") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_CROSSES:
	    		builder.append(argumentsSql.get(0)+".STCrosses("+argumentsSql.get(1)+")") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_DIFFERENCE:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STDifference("+argumentsSql.get(1)+")") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_DIMENSION:
	    		builder.append(argumentsSql.get(0)+".STDimension()") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_DISJOINT:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STDisjoint("+argumentsSql.get(1)+")") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_DISTANCE:
	    		builder.append(argumentsSql.get(0)+".STDistance("+argumentsSql.get(1)+")") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_ENVELOPE:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STEnvelope()") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_EQUALS:
	    		builder.append(argumentsSql.get(0)+".STEquals("+argumentsSql.get(1)+")") ;
	    		sql = builder.toString();
	    		break;
	
	
	    case ST_GEOMETRYTYPE:
	    		builder.append(argumentsSql.get(0)+".STGeometryType()") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_INTERSECTION:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STIntersection("+argumentsSql.get(1)+")") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_INTERSECTS:
	    		builder.append(argumentsSql.get(0)+".STIntersects("+argumentsSql.get(1)+")") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_ISEMPTY:
	    		builder.append(argumentsSql.get(0)+".STIsEmpty()") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_ISSIMPLE:
	    		builder.append(argumentsSql.get(0)+".STIsSimple()") ;
	    		sql = builder.toString();
	    		break;
	    case ST_OVERLAPS:
	    		builder.append(argumentsSql.get(0)+".STOverlaps("+argumentsSql.get(1)+")") ;
	    		sql = builder.toString();
	    		break;
	
	    case ST_SYMDIFFERENCE:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STSymDifference ("+argumentsSql.get(1)+")") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_TOUCHES:
	    		builder.append(argumentsSql.get(0)+".STTouches("+argumentsSql.get(1)+")") ;
	    		sql = builder.toString();
	    		break;
		
	    case ST_UNION:
				builder.append("CAST(");
	    		builder.append(argumentsSql.get(0)+".STUnion("+argumentsSql.get(1)+")") ;
        		builder.append("as VARCHAR("+SqlServerSqlDialect.maxSqlServerVarcharSize+") )");
	    		sql = builder.toString();
	    		break;
	
	    case ST_WITHIN:
	    		builder.append(argumentsSql.get(0)+".STWithin("+argumentsSql.get(1)+")") ;
	    		sql = builder.toString();
	    		break;
	    		
	    case BIT_AND:
	    	   builder.append(argumentsSql.get(0)+" & "+argumentsSql.get(1));
	    	   sql = builder.toString();
	    	   break;
	   
	    case BIT_OR:
	    	   builder.append(argumentsSql.get(0)+" | "+argumentsSql.get(1));
	    	   sql = builder.toString();
	    	   break;
	    	   
	    case BIT_XOR:
	    	   builder.append(argumentsSql.get(0)+" ^ "+argumentsSql.get(1));
	    	   sql = builder.toString();
	    	   break;
	    	   
	    case BIT_NOT:
	    	   builder.append("~ "+argumentsSql.get(0));
	    	   sql = builder.toString();
	    	   break;
	    	   
	    case HASH_MD5:
	    		builder.append("CONVERT(Char, HASHBYTES('MD5',"+argumentsSql.get(0)+"), 2)");
	    		sql = builder.toString();
	    		break;
	    case HASH_SHA1:
	    		builder.append("CONVERT(Char, HASHBYTES('SHA1',"+argumentsSql.get(0)+"), 2)");
	    		sql = builder.toString();
	    		break;
    		
	    case HASH_SHA:
	    		builder.append("CONVERT(Char, HASHBYTES('SHA',"+argumentsSql.get(0)+"), 2)");
	    		sql = builder.toString();
	    		break;
    		
	    case ZEROIFNULL:
    		builder.append("ISNULL("+argumentsSql.get(0)+",0)");
    		sql = builder.toString();
    		break;
		
        default:
            break;
        }


        return sql;
    }
    
}
