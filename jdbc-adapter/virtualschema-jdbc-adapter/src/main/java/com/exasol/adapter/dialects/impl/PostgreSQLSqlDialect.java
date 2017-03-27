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

public class PostgreSQLSqlDialect extends AbstractSqlDialect{


    public PostgreSQLSqlDialect(SqlDialectContext context) {
		super(context);
	}

	public static final String NAME = "POSTGRESQL";
	
	public static int maxPostgresSQLVarcharSize = 2000000; // Postgres limit actually is 1 GB, so we use as max the EXASOL limit
	
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
        cap.supportMainCapability(MainCapability.LIMIT);
        cap.supportMainCapability(MainCapability.LIMIT_WITH_OFFSET);
        
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
        // BOOL is not supported
        cap.supportLiteral(LiteralCapability.BOOL);
        cap.supportLiteral(LiteralCapability.NULL);
        cap.supportLiteral(LiteralCapability.DATE);
        cap.supportLiteral(LiteralCapability.TIMESTAMP);
        cap.supportLiteral(LiteralCapability.TIMESTAMP_UTC);
        cap.supportLiteral(LiteralCapability.DOUBLE);
        cap.supportLiteral(LiteralCapability.EXACTNUMERIC);
        cap.supportLiteral(LiteralCapability.STRING);
        //cap.supportLiteral(LiteralCapability.INTERVAL);
        
        
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
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_SAMP);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_SAMP_DISTINCT);
        
        cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE);
        cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_SAMP);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_SAMP_DISTINCT)	;
        
        cap.supportAggregateFunction(AggregateFunctionCapability.GROUP_CONCAT); // translated to string_agg

        
        //math functions
        // Standard Arithmetic Operators
        cap.supportScalarFunction(ScalarFunctionCapability.ADD);
        cap.supportScalarFunction(ScalarFunctionCapability.SUB);
        cap.supportScalarFunction(ScalarFunctionCapability.MULT);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOAT_DIV);

        // Unary prefix operators
        cap.supportScalarFunction(ScalarFunctionCapability.NEG);

        // Numeric functions
        cap.supportScalarFunction(ScalarFunctionCapability.ABS);
        cap.supportScalarFunction(ScalarFunctionCapability.ACOS);
        cap.supportScalarFunction(ScalarFunctionCapability.ASIN);
        cap.supportScalarFunction(ScalarFunctionCapability.ATAN);
        cap.supportScalarFunction(ScalarFunctionCapability.ATAN2);
        cap.supportScalarFunction(ScalarFunctionCapability.CEIL);
        cap.supportScalarFunction(ScalarFunctionCapability.COS);
        cap.supportScalarFunction(ScalarFunctionCapability.COSH);
        cap.supportScalarFunction(ScalarFunctionCapability.COT);
        cap.supportScalarFunction(ScalarFunctionCapability.DEGREES);
        cap.supportScalarFunction(ScalarFunctionCapability.DIV);
        cap.supportScalarFunction(ScalarFunctionCapability.EXP);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOOR);
        cap.supportScalarFunction(ScalarFunctionCapability.GREATEST);
        cap.supportScalarFunction(ScalarFunctionCapability.LEAST);
        cap.supportScalarFunction(ScalarFunctionCapability.LN);
        cap.supportScalarFunction(ScalarFunctionCapability.LOG);
        cap.supportScalarFunction(ScalarFunctionCapability.MOD);
        cap.supportScalarFunction(ScalarFunctionCapability.POWER);
        cap.supportScalarFunction(ScalarFunctionCapability.RADIANS);
        cap.supportScalarFunction(ScalarFunctionCapability.RAND);
        cap.supportScalarFunction(ScalarFunctionCapability.ROUND);
        cap.supportScalarFunction(ScalarFunctionCapability.SIGN);
        cap.supportScalarFunction(ScalarFunctionCapability.SIN);
        cap.supportScalarFunction(ScalarFunctionCapability.SINH);
        cap.supportScalarFunction(ScalarFunctionCapability.SQRT);
        cap.supportScalarFunction(ScalarFunctionCapability.TAN);
        cap.supportScalarFunction(ScalarFunctionCapability.TANH);
        cap.supportScalarFunction(ScalarFunctionCapability.TRUNC);
        
        
        // String Functions
        cap.supportScalarFunction(ScalarFunctionCapability.ASCII);
        cap.supportScalarFunction(ScalarFunctionCapability.BIT_LENGTH);
        cap.supportScalarFunction(ScalarFunctionCapability.CHR);
        //cap.supportScalarFunction(ScalarFunctionCapability.COLOGNE_PHONETIC);
        cap.supportScalarFunction(ScalarFunctionCapability.CONCAT);
        //cap.supportScalarFunction(ScalarFunctionCapability.DUMP);
        //cap.supportScalarFunction(ScalarFunctionCapability.EDIT_DISTANCE);
        //cap.supportScalarFunction(ScalarFunctionCapability.INSERT);
        cap.supportScalarFunction(ScalarFunctionCapability.INSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.LENGTH);
        //cap.supportScalarFunction(ScalarFunctionCapability.LOCATE);
        cap.supportScalarFunction(ScalarFunctionCapability.LOWER);
        cap.supportScalarFunction(ScalarFunctionCapability.LPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.LTRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.OCTET_LENGTH);
        //cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_INSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_REPLACE);
        //cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_SUBSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.REPEAT);
        cap.supportScalarFunction(ScalarFunctionCapability.REPLACE);
        cap.supportScalarFunction(ScalarFunctionCapability.REVERSE);
        cap.supportScalarFunction(ScalarFunctionCapability.RIGHT);
        cap.supportScalarFunction(ScalarFunctionCapability.RPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.RTRIM);
        //cap.supportScalarFunction(ScalarFunctionCapability.SOUNDEX);
        //cap.supportScalarFunction(ScalarFunctionCapability.SPACE);
        cap.supportScalarFunction(ScalarFunctionCapability.SUBSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.TRANSLATE);
        cap.supportScalarFunction(ScalarFunctionCapability.TRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.UNICODE);
        cap.supportScalarFunction(ScalarFunctionCapability.UNICODECHR);
        cap.supportScalarFunction(ScalarFunctionCapability.UPPER);
        
        // Date/Time Functions
        
        //The following functions will be rewrited to + operator in the Visitor
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_DAYS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_HOURS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MINUTES);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MONTHS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_SECONDS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_WEEKS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_YEARS);
        
        //cap.supportScalarFunction(ScalarFunctionCapability.CONVERT_TZ);
        
        
        //handled via Visitor and transformed to e.g. date_part('day',age('2012-03-05','2010-04-01' ))
        cap.supportScalarFunction(ScalarFunctionCapability.SECONDS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.MINUTES_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.HOURS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.DAYS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.MONTHS_BETWEEN);
        cap.supportScalarFunction(ScalarFunctionCapability.YEARS_BETWEEN);
        
        //handled via Visitor and transformed to e.g. date_part
        cap.supportScalarFunction(ScalarFunctionCapability.MINUTE);
        cap.supportScalarFunction(ScalarFunctionCapability.SECOND);
        cap.supportScalarFunction(ScalarFunctionCapability.DAY);
        cap.supportScalarFunction(ScalarFunctionCapability.WEEK);
        cap.supportScalarFunction(ScalarFunctionCapability.MONTH);
        cap.supportScalarFunction(ScalarFunctionCapability.YEAR);


        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_DATE);
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_TIMESTAMP);
        cap.supportScalarFunction(ScalarFunctionCapability.DATE_TRUNC);
        
        //cap.supportScalarFunction(ScalarFunctionCapability.DBTIMEZONE);
        cap.supportScalarFunction(ScalarFunctionCapability.EXTRACT);
        cap.supportScalarFunction(ScalarFunctionCapability.LOCALTIMESTAMP);
        //cap.supportScalarFunction(ScalarFunctionCapability.NUMTODSINTERVAL);
        //cap.supportScalarFunction(ScalarFunctionCapability.NUMTOYMINTERVAL);
        cap.supportScalarFunction(ScalarFunctionCapability.POSIX_TIME); //converted to extract(epoche
        //cap.supportScalarFunction(ScalarFunctionCapability.SESSIONTIMEZONE);
        //cap.supportScalarFunction(ScalarFunctionCapability.SYSDATE);
        //cap.supportScalarFunction(ScalarFunctionCapability.SYSTIMESTAMP);

        // Conversion functions
//        cap.supportScalarFunction(ScalarFunctionCapability.IS_NUMBER);
//        cap.supportScalarFunction(ScalarFunctionCapability.IS_BOOLEAN);
//        cap.supportScalarFunction(ScalarFunctionCapability.IS_DATE);
//        cap.supportScalarFunction(ScalarFunctionCapability.IS_DSINTERVAL);
//        cap.supportScalarFunction(ScalarFunctionCapability.IS_YMINTERVAL);
//        cap.supportScalarFunction(ScalarFunctionCapability.IS_TIMESTAMP);
        cap.supportScalarFunction(ScalarFunctionCapability.TO_CHAR);
//        cap.supportScalarFunction(ScalarFunctionCapability.TO_DATE);
//        cap.supportScalarFunction(ScalarFunctionCapability.TO_DSINTERVAL);
//        cap.supportScalarFunction(ScalarFunctionCapability.TO_YMINTERVAL);
//        cap.supportScalarFunction(ScalarFunctionCapability.TO_NUMBER);
//        cap.supportScalarFunction(ScalarFunctionCapability.TO_TIMESTAMP);
        
        // Bitwise functions
//        cap.supportScalarFunction(ScalarFunctionCapability.BIT_AND);
//        cap.supportScalarFunction(ScalarFunctionCapability.BIT_CHECK);
//        cap.supportScalarFunction(ScalarFunctionCapability.BIT_NOT);
//        cap.supportScalarFunction(ScalarFunctionCapability.BIT_OR);
//        cap.supportScalarFunction(ScalarFunctionCapability.BIT_SET);
//        cap.supportScalarFunction(ScalarFunctionCapability.BIT_TO_NUM);
//        cap.supportScalarFunction(ScalarFunctionCapability.BIT_XOR);
        
        
     // Other functions
        cap.supportScalarFunction(ScalarFunctionCapability.CASE);
//        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_SCHEMA);
//        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_SESSION);
//        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_STATEMENT);
//        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_USER);
        cap.supportScalarFunction(ScalarFunctionCapability.HASH_MD5);
//        cap.supportScalarFunction(ScalarFunctionCapability.HASH_SHA);
//        cap.supportScalarFunction(ScalarFunctionCapability.HASH_SHA1);
//        cap.supportScalarFunction(ScalarFunctionCapability.HASH_TIGER);
//        cap.supportScalarFunction(ScalarFunctionCapability.NULLIFZERO);
//        cap.supportScalarFunction(ScalarFunctionCapability.SYS_GUID);
//        cap.supportScalarFunction(ScalarFunctionCapability.ZEROIFNULL);
        
        return cap;
	}
	
	 @Override
    public DataType mapJdbcType(ResultSet cols) throws SQLException {
        DataType colType = null;
        int jdbcType = cols.getInt("DATA_TYPE");

        switch (jdbcType) {
        		case Types.OTHER:
	            	String columnTypeName = cols.getString("TYPE_NAME");
	
	            	if(columnTypeName.equals("varbit")){
	            		int n = cols.getInt("COLUMN_SIZE");
	            		colType = DataType.createVarChar(n, DataType.ExaCharset.UTF8);
	            	}
	            	else
	            		colType = DataType.createVarChar(PostgreSQLSqlDialect.maxPostgresSQLVarcharSize, DataType.ExaCharset.UTF8);     
	                break;
        		case Types.SQLXML:
	            		colType = DataType.createVarChar(PostgreSQLSqlDialect.maxPostgresSQLVarcharSize, DataType.ExaCharset.UTF8);
        			break;
        }
        
        return colType;
    }
	
	@Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
		
		Map<ScalarFunction,String> scalarAliases = new EnumMap<>(ScalarFunction.class); 
		
		scalarAliases.put(ScalarFunction.SUBSTR,"SUBSTRING");
		scalarAliases.put(ScalarFunction.HASH_MD5, "MD5");
		
		return scalarAliases;
		
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
    public String changeIdentifierCaseIfNeeded(String identifier) {
		
		boolean isSimplePostgresIdentifier = identifier.matches("^[a-z][0-9a-z_]*");
		
		 if(isSimplePostgresIdentifier) 
			 return identifier.toUpperCase();
		 else
			 return identifier;
		 
    }
	
	@Override
	public IdentifierCaseHandling getUnquotedIdentifierHandling() {
		 return IdentifierCaseHandling.INTERPRET_AS_LOWER;
	}

	@Override
	public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
	}

	
	@Override
	public String applyQuote(String identifier) {
		return "\"" + identifier.replace("\"", "\"\"") + "\"";
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
		return false;
	}

	
	
	@Override
	public boolean requiresSchemaQualifiedTableNames(
			SqlGenerationContext context) {
		return true;
	}

	@Override
	public NullSorting getDefaultNullSorting() {
		return NullSorting.NULLS_SORTED_AT_END;
	}

	@Override
	public String getStringLiteral(String value) {
		 return "'" + value.replace("'", "''") + "'";
	}
	
	 @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(SqlGenerationContext context) {
        return new PostgresSQLSqlGenerationVisitor(this, context);
    }

}
