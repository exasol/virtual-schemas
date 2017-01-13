package com.exasol.adapter.dialects.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

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


public class TeradataSqlDialect extends AbstractSqlDialect{

	public final static int maxTeradataVarcharSize = 32000;  
	
    public TeradataSqlDialect(SqlDialectContext context) {
		super(context);
	}

	public static final String NAME = "TERADATA";
	
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
        // GROUP_CONCAT is not supported
        // GEO_INTERSECTION_AGGREGATE is not supported
        // GEO_UNION_AGGREGATE is not supported
        // APPROXIMATE_COUNT_DISTINCT not supported
        
        cap.supportAggregateFunction(AggregateFunctionCapability.SUM);
        cap.supportAggregateFunction(AggregateFunctionCapability.SUM_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.MIN);
        cap.supportAggregateFunction(AggregateFunctionCapability.MAX);
        cap.supportAggregateFunction(AggregateFunctionCapability.AVG);
        cap.supportAggregateFunction(AggregateFunctionCapability.AVG_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.MEDIAN);
        cap.supportAggregateFunction(AggregateFunctionCapability.FIRST_VALUE);
        cap.supportAggregateFunction(AggregateFunctionCapability.LAST_VALUE);
        //cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV);
        //cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_POP);
        // STDDEV_POP_DISTINCT
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_SAMP);
        // STDDEV_SAMP_DISTINCT
        //cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE);
        //cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP);
        // VAR_POP_DISTINCT
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_SAMP);
        // VAR_SAMP_DISTINCT
        
        cap.supportScalarFunction(ScalarFunctionCapability.CEIL);
        cap.supportScalarFunction(ScalarFunctionCapability.DIV);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOOR);
        cap.supportScalarFunction(ScalarFunctionCapability.ROUND);
        cap.supportScalarFunction(ScalarFunctionCapability.SIGN);
        cap.supportScalarFunction(ScalarFunctionCapability.TRUNC);
        
        cap.supportScalarFunction(ScalarFunctionCapability.ADD);
        cap.supportScalarFunction(ScalarFunctionCapability.SUB);
        cap.supportScalarFunction(ScalarFunctionCapability.MULT);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOAT_DIV);
        cap.supportScalarFunction(ScalarFunctionCapability.NEG);
        cap.supportScalarFunction(ScalarFunctionCapability.ABS);
        cap.supportScalarFunction(ScalarFunctionCapability.ACOS);
        cap.supportScalarFunction(ScalarFunctionCapability.ASIN);
        cap.supportScalarFunction(ScalarFunctionCapability.ATAN);
        cap.supportScalarFunction(ScalarFunctionCapability.ATAN2);
        cap.supportScalarFunction(ScalarFunctionCapability.COS);
        cap.supportScalarFunction(ScalarFunctionCapability.COSH);
        cap.supportScalarFunction(ScalarFunctionCapability.COT);
        cap.supportScalarFunction(ScalarFunctionCapability.DEGREES);
        cap.supportScalarFunction(ScalarFunctionCapability.EXP);
        cap.supportScalarFunction(ScalarFunctionCapability.GREATEST);
        cap.supportScalarFunction(ScalarFunctionCapability.LEAST);
        cap.supportScalarFunction(ScalarFunctionCapability.LN);
        cap.supportScalarFunction(ScalarFunctionCapability.LOG);
        cap.supportScalarFunction(ScalarFunctionCapability.MOD);
        cap.supportScalarFunction(ScalarFunctionCapability.POWER);
        cap.supportScalarFunction(ScalarFunctionCapability.RADIANS);
        // RAND is not supported (constant arguments in EXA, will not be pushed down)
        cap.supportScalarFunction(ScalarFunctionCapability.SIN);
        cap.supportScalarFunction(ScalarFunctionCapability.SINH);
        cap.supportScalarFunction(ScalarFunctionCapability.SQRT);
        cap.supportScalarFunction(ScalarFunctionCapability.TAN);
        cap.supportScalarFunction(ScalarFunctionCapability.TANH);
        
       
        cap.supportScalarFunction(ScalarFunctionCapability.ASCII); 
        // BIT_LENGTH is not supported. Can be different for Unicode characters.
        cap.supportScalarFunction(ScalarFunctionCapability.CHR);
        // COLOGNE_PHONETIC is not supported.
        // CONCAT is not supported. Number of arguments can be different.
        // DUMP is not supported. Output is different.
        // EDIT_DISTANCE is not supported. Output is different. UTL_MATCH.EDIT_DISTANCE returns -1 with NULL argument.
        // INSERT is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.INSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.LENGTH);
        cap.supportScalarFunction(ScalarFunctionCapability.LOCATE);
        cap.supportScalarFunction(ScalarFunctionCapability.LOWER);
        cap.supportScalarFunction(ScalarFunctionCapability.LPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.LTRIM);
        // OCTET_LENGTH is not supported. Can be different for Unicode characters.
        cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_INSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_REPLACE);
        cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_SUBSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.REPEAT);
        cap.supportScalarFunction(ScalarFunctionCapability.REPLACE);
        cap.supportScalarFunction(ScalarFunctionCapability.REVERSE);
        // RIGHT is not supported. Possible solution with SUBSTRING (must handle corner cases correctly).
        cap.supportScalarFunction(ScalarFunctionCapability.RPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.RTRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.SOUNDEX);
        // SPACE is not supported. Parameter = 0 has different results from RPAD.
        cap.supportScalarFunction(ScalarFunctionCapability.SUBSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.TRANSLATE);
        cap.supportScalarFunction(ScalarFunctionCapability.TRIM);
        // UNICODE is not supported.
        // UNICODECHR is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.UPPER);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_DAYS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_HOURS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MINUTES);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_MONTHS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_SECONDS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_WEEKS);
        cap.supportScalarFunction(ScalarFunctionCapability.ADD_YEARS);
        
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_DATE);
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_TIMESTAMP);
        
        cap.supportScalarFunction(ScalarFunctionCapability.NULLIFZERO);
        cap.supportScalarFunction(ScalarFunctionCapability.ZEROIFNULL);
        
        return cap;
	}

	
    @Override
    public DataType mapJdbcType(ResultSet cols) throws SQLException {
        DataType colType = null;
        int jdbcType = cols.getInt("DATA_TYPE");
        switch (jdbcType) {
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
            case Types.OTHER: // Teradata JDBC uses OTHER for several data types GEOMETRY, INTERVAL etc...  
            	String columnTypeName = cols.getString("TYPE_NAME");
            	
            	 if ( columnTypeName.equals("GEOMETRY") )
            		 colType = DataType.createVarChar(cols.getInt("COLUMN_SIZE"), DataType.ExaCharset.UTF8);
            	 else if (columnTypeName.startsWith("INTERVAL") )
            		 colType = DataType.createVarChar(30, DataType.ExaCharset.UTF8); //TODO verify that varchar 30 is sufficient in all cases
            	 else if (columnTypeName.startsWith("PERIOD") )
            		 colType = DataType.createVarChar(100, DataType.ExaCharset.UTF8); 
            	 else
            		 colType = DataType.createVarChar(TeradataSqlDialect.maxTeradataVarcharSize, DataType.ExaCharset.UTF8);     
            	 break;
            	 
            case Types.SQLXML:
           	 	colType = DataType.createVarChar(TeradataSqlDialect.maxTeradataVarcharSize, DataType.ExaCharset.UTF8);
            	break;
            	
            case Types.CLOB:
            	colType = DataType.createVarChar(TeradataSqlDialect.maxTeradataVarcharSize, DataType.ExaCharset.UTF8);
            	break;
            	
            case Types.BLOB:
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
	public SchemaOrCatalogSupport supportsJdbcCatalogs() {
        return SchemaOrCatalogSupport.UNSUPPORTED;
	}

	@Override
	public SchemaOrCatalogSupport supportsJdbcSchemas() {
        return SchemaOrCatalogSupport.SUPPORTED;
	}

	@Override
    public SqlGenerationVisitor getSqlGenerationVisitor(SqlGenerationContext context) {
        return new TeradataSqlGenerationVisitor(this, context);
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
		return NullSorting.NULLS_SORTED_HIGH;
	}

	@Override
	public String getStringLiteral(String value) {
		 return "'" + value.replace("'", "''") + "'";
	}

}
