package com.exasol.adapter.dialects.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.EnumMap;
import java.util.Map;

import com.exasol.adapter.capabilities.AggregateFunctionCapability;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.LiteralCapability;
import com.exasol.adapter.capabilities.MainCapability;
import com.exasol.adapter.capabilities.PredicateCapability;
import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.AbstractSqlDialect;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;


public class SqlServerSqlDialect extends AbstractSqlDialect{
	
	
	// Tested SQL Server versions: SQL Server 2014
	// Tested JDBC drivers:  jtds-1.3.1 (https://sourceforge.net/projects/jtds/)

	public final static int maxSqlServerVarcharSize = 8000;  
	
	public final static int maxSqlServerNVarcharSize = 4000;  

	
    public SqlServerSqlDialect(SqlDialectContext context) {
		super(context);
	}

	public static final String NAME = "SQLSERVER";
	
	@Override
	public String getPublicName() {
		return NAME;
	}

	@Override
	public Capabilities getCapabilities() {
	
        Capabilities cap = new Capabilities();

        cap.supportMainCapability(MainCapability.SELECTLIST_PROJECTION);
        cap.supportMainCapability(MainCapability.SELECTLIST_EXPRESSIONS);
        cap.supportMainCapability(MainCapability.FILTER_EXPRESSIONS);
        cap.supportMainCapability(MainCapability.AGGREGATE_SINGLE_GROUP);
        cap.supportMainCapability(MainCapability.AGGREGATE_GROUP_BY_COLUMN);
        cap.supportMainCapability(MainCapability.AGGREGATE_GROUP_BY_EXPRESSION);
        cap.supportMainCapability(MainCapability.AGGREGATE_GROUP_BY_TUPLE);
        cap.supportMainCapability(MainCapability.AGGREGATE_HAVING);
        cap.supportMainCapability(MainCapability.ORDER_BY_COLUMN);
        cap.supportMainCapability(MainCapability.ORDER_BY_EXPRESSION);
        cap.supportMainCapability(MainCapability.LIMIT); // LIMIT will be translated to TOP in SqlServerSqlGenerationVisitor.java
        

        // Predicates
        cap.supportPredicate(PredicateCapability.AND);
        cap.supportPredicate(PredicateCapability.OR);
        cap.supportPredicate(PredicateCapability.NOT);
        cap.supportPredicate(PredicateCapability.EQUAL);
        cap.supportPredicate(PredicateCapability.NOTEQUAL);
        cap.supportPredicate(PredicateCapability.LESS);
        cap.supportPredicate(PredicateCapability.LESSEQUAL);
        cap.supportPredicate(PredicateCapability.LIKE);
        cap.supportPredicate(PredicateCapability.LIKE_ESCAPE);
        cap.supportPredicate(PredicateCapability.REGEXP_LIKE);
        cap.supportPredicate(PredicateCapability.BETWEEN);
        cap.supportPredicate(PredicateCapability.IN_CONSTLIST);
        cap.supportPredicate(PredicateCapability.IS_NULL);
        cap.supportPredicate(PredicateCapability.IS_NOT_NULL);
        
        // Literals
        cap.supportLiteral(LiteralCapability.BOOL);
        cap.supportLiteral(LiteralCapability.NULL);
        cap.supportLiteral(LiteralCapability.DATE);
        cap.supportLiteral(LiteralCapability.TIMESTAMP);
        cap.supportLiteral(LiteralCapability.TIMESTAMP_UTC);
        cap.supportLiteral(LiteralCapability.DOUBLE);
        cap.supportLiteral(LiteralCapability.EXACTNUMERIC);
        cap.supportLiteral(LiteralCapability.STRING);
        cap.supportLiteral(LiteralCapability.INTERVAL);
        
        // Aggregate functions
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_STAR);
        cap.supportAggregateFunction(AggregateFunctionCapability.COUNT_DISTINCT);

        cap.supportAggregateFunction(AggregateFunctionCapability.SUM);
        cap.supportAggregateFunction(AggregateFunctionCapability.SUM_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.MIN);
        cap.supportAggregateFunction(AggregateFunctionCapability.MAX);
        cap.supportAggregateFunction(AggregateFunctionCapability.AVG);
        cap.supportAggregateFunction(AggregateFunctionCapability.AVG_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.MEDIAN);
        cap.supportAggregateFunction(AggregateFunctionCapability.FIRST_VALUE);
        cap.supportAggregateFunction(AggregateFunctionCapability.LAST_VALUE);
        
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_POP);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_POP_DISTINCT);

        // STDDEV_SAMP
        // STDDEV_SAMP_DISTINCT
        
        cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE);
        cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE_DISTINCT);
        
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP_DISTINCT);

        
		//        GROUP_CONCAT,
		//        GROUP_CONCAT_DISTINCT (AggregateFunction.GROUP_CONCAT),
		//        GROUP_CONCAT_SEPARATOR (AggregateFunction.GROUP_CONCAT),
		//        GROUP_CONCAT_ORDER_BY (AggregateFunction.GROUP_CONCAT),
		//
		//        GEO_INTERSECTION_AGGREGATE,
		//        GEO_UNION_AGGREGATE,
		//
		//        APPROXIMATE_COUNT_DISTINCT;
        
        

        // Standard Arithmetic Operators
        cap.supportScalarFunction(ScalarFunctionCapability.ADD);
        cap.supportScalarFunction(ScalarFunctionCapability.SUB);
        cap.supportScalarFunction(ScalarFunctionCapability.MULT);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOAT_DIV);

        // Unary prefix operators
        cap.supportScalarFunction(ScalarFunctionCapability.NEG);

        // Numeric functions  https://msdn.microsoft.com/en-us/library/ms177516(v=sql.110).aspx
        cap.supportScalarFunction(ScalarFunctionCapability.ABS);
        cap.supportScalarFunction(ScalarFunctionCapability.ACOS);
        cap.supportScalarFunction(ScalarFunctionCapability.ASIN);
        cap.supportScalarFunction(ScalarFunctionCapability.ATAN);
        cap.supportScalarFunction(ScalarFunctionCapability.ATAN2); // added alias ATN2
        cap.supportScalarFunction(ScalarFunctionCapability.CEIL); //alias CEILING
        cap.supportScalarFunction(ScalarFunctionCapability.COS);
        //COSH
        cap.supportScalarFunction(ScalarFunctionCapability.COT);
        cap.supportScalarFunction(ScalarFunctionCapability.DEGREES);
        //DIV,
        cap.supportScalarFunction(ScalarFunctionCapability.EXP);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOOR);
        //GREATEST,
        //LEAST,
        //LN,
        cap.supportScalarFunction(ScalarFunctionCapability.LOG);
        cap.supportScalarFunction(ScalarFunctionCapability.MOD);
        cap.supportScalarFunction(ScalarFunctionCapability.POWER);
        cap.supportScalarFunction(ScalarFunctionCapability.RADIANS);
        cap.supportScalarFunction(ScalarFunctionCapability.RAND);
        cap.supportScalarFunction(ScalarFunctionCapability.ROUND);
        cap.supportScalarFunction(ScalarFunctionCapability.SIGN);
        cap.supportScalarFunction(ScalarFunctionCapability.SIN);
        //SINH,
        cap.supportScalarFunction(ScalarFunctionCapability.SQRT);
        cap.supportScalarFunction(ScalarFunctionCapability.TAN);
        //TANH,
        cap.supportScalarFunction(ScalarFunctionCapability.TRUNC);
        
        // String Functions 
        cap.supportScalarFunction(ScalarFunctionCapability.ASCII);
        //BIT_LENGTH,
        cap.supportScalarFunction(ScalarFunctionCapability.CHR); //CHAR
        //COLOGNE_PHONETIC,
        cap.supportScalarFunction(ScalarFunctionCapability.CONCAT);
        //DUMP,
        //EDIT_DISTANCE,
        //INSERT,
        cap.supportScalarFunction(ScalarFunctionCapability.INSTR); //  translated to CHARINDEX in Visitor with Argument switch
        cap.supportScalarFunction(ScalarFunctionCapability.LENGTH); //alias LEN
        cap.supportScalarFunction(ScalarFunctionCapability.LOCATE); // CHARINDEX alias   
        cap.supportScalarFunction(ScalarFunctionCapability.LOWER);
        cap.supportScalarFunction(ScalarFunctionCapability.LPAD); //transformed in Visitor
        cap.supportScalarFunction(ScalarFunctionCapability.LTRIM);
        //OCTET_LENGTH,
        //REGEXP_INSTR,
        //REGEXP_REPLACE,
        //REGEXP_SUBSTR,
        cap.supportScalarFunction(ScalarFunctionCapability.REPEAT); //REPLICATE
        cap.supportScalarFunction(ScalarFunctionCapability.REPLACE);
        cap.supportScalarFunction(ScalarFunctionCapability.REVERSE);
        cap.supportScalarFunction(ScalarFunctionCapability.RIGHT);
        cap.supportScalarFunction(ScalarFunctionCapability.RPAD); 
        cap.supportScalarFunction(ScalarFunctionCapability.RTRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.SOUNDEX); 
        cap.supportScalarFunction(ScalarFunctionCapability.SPACE);
        cap.supportScalarFunction(ScalarFunctionCapability.SUBSTR); //SUBSTRING
        //TRANSLATE,
        cap.supportScalarFunction(ScalarFunctionCapability.TRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.UNICODE);
        //UNICODECHR,
        cap.supportScalarFunction(ScalarFunctionCapability.UPPER);
 
 
        // Date/Time Functions
        
        
        // the following functions are translated to DATEADD(datepart,number,date) in Visitor
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_DAYS);   
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_HOURS);   
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MINUTES);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MONTHS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_SECONDS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_WEEKS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_YEARS);  
        
        //CONVERT_TZ,
           
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_DATE);  
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_TIMESTAMP);
        
        //DATE_TRUNC,
        cap.supportScalarFunction(ScalarFunctionCapability.DAY);
        
        //the following functions are translated to DATEDIFF in Visitor
        cap.supportScalarFunction(ScalarFunctionCapability.SECONDS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.MINUTES_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.HOURS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.DAYS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.MONTHS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.YEARS_BETWEEN);
        
        
//        DBTIMEZONE,
//        EXTRACT,
//        LOCALTIMESTAMP,
//        MINUTE,

        cap.supportScalarFunction(ScalarFunctionCapability.MONTH);

//        NUMTODSINTERVAL,
//        NUMTOYMINTERVAL,
//        POSIX_TIME,
//        SECOND,

//        SESSIONTIMEZONE,
        cap.supportScalarFunction(ScalarFunctionCapability.SYSDATE);
        cap.supportScalarFunction(ScalarFunctionCapability.SYSTIMESTAMP);
        
//        WEEK,
        
        cap.supportScalarFunction(ScalarFunctionCapability.YEAR);
        

        // Geospatial
        // - Point Functions
        cap.supportScalarFunction(ScalarFunctionCapability.ST_X);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_Y);
//        // - (Multi-)LineString Functions
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ENDPOINT);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ISCLOSED);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ISRING);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_LENGTH);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_NUMPOINTS);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_POINTN);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_STARTPOINT);
//        // - (Multi-)Polygon Functions
        cap.supportScalarFunction(ScalarFunctionCapability.ST_AREA);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_EXTERIORRING);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_INTERIORRINGN);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_NUMINTERIORRINGS);
//        // - GeometryCollection Functions
        cap.supportScalarFunction(ScalarFunctionCapability.ST_GEOMETRYN);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_NUMGEOMETRIES);
//        // - General Functions
        cap.supportScalarFunction(ScalarFunctionCapability.ST_BOUNDARY);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_BUFFER);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_CENTROID);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_CONTAINS);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_CONVEXHULL);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_CROSSES);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_DIFFERENCE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_DIMENSION);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_DISJOINT);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_DISTANCE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ENVELOPE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_EQUALS);
        //cap.supportScalarFunction(ScalarFunctionCapability.ST_FORCE2D);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_GEOMETRYTYPE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_INTERSECTION);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_INTERSECTS);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ISEMPTY);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_ISSIMPLE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_OVERLAPS);
        //cap.supportScalarFunction(ScalarFunctionCapability.ST_SETSRID);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_SYMDIFFERENCE);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_TOUCHES);
        //cap.supportScalarFunction(ScalarFunctionCapability.ST_TRANSFORM);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_UNION);
        cap.supportScalarFunction(ScalarFunctionCapability.ST_WITHIN);
        
        // Conversion functions
//        CAST,  // Has alias CONVERT
//		  IS_NUMBER
//        IS_BOOLEAN,
//        IS_DATE,
//        IS_DSINTERVAL,
//        IS_YMINTERVAL,
//        IS_TIMESTAMP,
//        TO_CHAR,
//        TO_DATE,
//        TO_DSINTERVAL,
//        TO_YMINTERVAL,
//        TO_NUMBER,
//        TO_TIMESTAMP,
        
        // Bitwise functions
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_AND);
//        BIT_CHECK,
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_NOT);
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_OR);
//        BIT_SET,
//        BIT_TO_NUM,
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_XOR);

        // Other functions
        cap.supportScalarFunction(ScalarFunctionCapability.CASE);
//        CURRENT_SCHEMA,
//        CURRENT_SESSION,
//        CURRENT_STATEMENT,
//        CURRENT_USER,
        cap.supportScalarFunction(ScalarFunctionCapability.HASH_MD5); //translated to HASHBYTES
        cap.supportScalarFunction(ScalarFunctionCapability.HASH_SHA); //translated to HASHBYTES
        cap.supportScalarFunction(ScalarFunctionCapability.HASH_SHA1); //translated to HASHBYTES
//        HASH_TIGER, 
        cap.supportScalarFunction(ScalarFunctionCapability.NULLIFZERO); //alias NULLIF
//        SYS_GUID,
        cap.supportScalarFunction(ScalarFunctionCapability.ZEROIFNULL); //translated to ISNULL(exp1, exp2) in Visitor

        return cap;
	}

	
    @Override
    public DataType mapJdbcType(ResultSet cols) throws SQLException {
        DataType colType = null;
        int jdbcType = cols.getInt("DATA_TYPE");
        String columnTypeName = cols.getString("TYPE_NAME");
        
        switch (jdbcType) {
        
        	case Types.VARCHAR:  //the JTDS JDBC Type for date, time, datetime2, datetimeoffset is 12
        		if(columnTypeName.equalsIgnoreCase("date")) {
        			colType = DataType.createDate();
        		}
        		else if(columnTypeName.equalsIgnoreCase("datetime2")) {
        			colType = DataType.createTimestamp(false);
        		} 
        		
        		//note: time and datetimeoffset are converted to varchar by default mapping
        		
        		break;
        	case Types.TIME:
        		colType = DataType.createVarChar(21, DataType.ExaCharset.UTF8);
        		break;
        	case 2013: //Types.TIME_WITH_TIMEZONE is Java 1.8 specific
        		colType = DataType.createVarChar(21, DataType.ExaCharset.UTF8);
        		break;
        	case Types.NUMERIC:
        		int decimalPrec = cols.getInt("COLUMN_SIZE");
                int decimalScale = cols.getInt("DECIMAL_DIGITS");

                if (decimalPrec <= DataType.maxExasolDecimalPrecision) {
                    colType = DataType.createDecimal(decimalPrec, decimalScale);
                } else {
                    colType = DataType.createDouble();
                }
                break;
            case Types.OTHER: 
            	
            	//TODO
            		 colType = DataType.createVarChar(SqlServerSqlDialect.maxSqlServerVarcharSize, DataType.ExaCharset.UTF8);     
            	 break;
            	 
            case Types.SQLXML:

           	 	colType = DataType.createVarChar(SqlServerSqlDialect.maxSqlServerVarcharSize, DataType.ExaCharset.UTF8);
            	break;
            	
            case Types.CLOB: //xml type in SQL Server
            	
            	colType = DataType.createVarChar(SqlServerSqlDialect.maxSqlServerNVarcharSize, DataType.ExaCharset.UTF8);
            	break;
            	
            case Types.BLOB:
        		if(columnTypeName.equalsIgnoreCase("hierarchyid")) {
   	       		 	colType = DataType.createVarChar(4000, DataType.ExaCharset.UTF8);
        		}
   	       		if(columnTypeName.equalsIgnoreCase("geometry")) {
   	       		 	colType = DataType.createVarChar(SqlServerSqlDialect.maxSqlServerVarcharSize, DataType.ExaCharset.UTF8);  	
        		}
   	       		else{
   	       			colType = DataType.createVarChar(100, DataType.ExaCharset.UTF8);
        		}
        		break;
            case Types.VARBINARY:
            case Types.BINARY:
	       		 colType = DataType.createVarChar(100, DataType.ExaCharset.UTF8); 
	       		 break;
            case Types.DISTINCT:
            	colType = DataType.createVarChar(100, DataType.ExaCharset.UTF8);
            	break;
        }
        return colType;
    }
	
	
	
	@Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
		
		Map<ScalarFunction,String> scalarAliases = new EnumMap<>(ScalarFunction.class); 
		
		scalarAliases.put(ScalarFunction.ATAN2, 	"ATN2");
		scalarAliases.put(ScalarFunction.CEIL, 		"CEILING");
		scalarAliases.put(ScalarFunction.CHR, 		"CHAR");
		scalarAliases.put(ScalarFunction.LENGTH, 	"LEN");
		scalarAliases.put(ScalarFunction.LOCATE, 	"CHARINDEX");
		scalarAliases.put(ScalarFunction.REPEAT, 	"REPLICATE");
		scalarAliases.put(ScalarFunction.SUBSTR, 	"SUBSTRING");	
		scalarAliases.put(ScalarFunction.NULLIFZERO, "NULLIF");
		
		return scalarAliases;
		
	}
    
	@Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        Map<AggregateFunction, String> aggregationAliases = new EnumMap<>(AggregateFunction.class);
        
        aggregationAliases.put(AggregateFunction.STDDEV, "STDEV");

        aggregationAliases.put(AggregateFunction.STDDEV_POP, "STDEVP");
        
        aggregationAliases.put(AggregateFunction.VARIANCE, "VAR");
        
        aggregationAliases.put(AggregateFunction.VAR_POP, "VARP");
        
        return aggregationAliases;
    }
    
	@Override
	public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.SUPPORTED;
	}

	@Override
	public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.SUPPORTED;
	}

	@Override
    public SqlGenerationVisitor getSqlGenerationVisitor(SqlGenerationContext context) {
        return new SqlServerSqlGenerationVisitor(this, context);
    }
	
	@Override
	public IdentifierCaseHandling getUnquotedIdentifierHandling() {
		 return IdentifierCaseHandling.INTERPRET_AS_UPPER;
	}

	@Override
	public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
	}

	@Override
	public String applyQuote(String identifier) {
		return "[" + identifier + "]";
	}

	@Override
	public String applyQuoteIfNeeded(String identifier) {
		 boolean isSimpleIdentifier = identifier.matches("^[A-Z][0-9A-Z_]*");
	        if (isSimpleIdentifier) {
	            return identifier;
	        } else {
	            return applyQuote(identifier);
	        }
	}

	@Override
	public boolean requiresCatalogQualifiedTableNames(
			SqlGenerationContext context) {
		return true;
	}

	@Override
	public boolean requiresSchemaQualifiedTableNames(
			SqlGenerationContext context) {
		return true;
	}

	@Override
	public NullSorting getDefaultNullSorting() {
		return NullSorting.NULLS_SORTED_AT_START;
	}

	@Override
	public String getStringLiteral(String value) {
		 return "'" + value.replace("'", "''") + "'";
	}

}
