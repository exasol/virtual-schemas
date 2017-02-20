package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.dialects.AbstractSqlDialect;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import com.exasol.adapter.capabilities.AggregateFunctionCapability;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.LiteralCapability;
import com.exasol.adapter.capabilities.MainCapability;
import com.exasol.adapter.capabilities.PredicateCapability;
import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.metadata.DataType;

/**
 * Dialect for DB2 using the DB2 Connector jdbc driver.
 *
 * @author Karl Griesser (fullref@gmail.com)
 */

public class DB2SqlDialect extends AbstractSqlDialect {

    public DB2SqlDialect(SqlDialectContext context)
    {
        super(context);
    }

    public static final String NAME = "DB2";
    
    @Override
    public String getPublicName()
    {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities()
    {
        Capabilities cap = new Capabilities();
        // Capabilities
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
        // not supported cap.supportPredicate(PredicateCapability.REGEXP_LIKE);
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
        cap.supportAggregateFunction(AggregateFunctionCapability.GROUP_CONCAT);
        // GROUP_CONCAT_DISTINCT is supported
        cap.supportAggregateFunction(AggregateFunctionCapability.GROUP_CONCAT_SEPARATOR);
        cap.supportAggregateFunction(AggregateFunctionCapability.GROUP_CONCAT_ORDER_BY);
        // GEO_INTERSECTION_AGGREGATE is not supported
        // GEO_UNION_AGGREGATE is not supported
        // APPROXIMATE_COUNT_DISTINCT supported with version >= 12.1.0.2
        // Cast result to FLOAT because result set precision = 0, scale = 0
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
        // not supported  cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_POP);
        // STDDEV_POP_DISTINCT
        cap.supportAggregateFunction(AggregateFunctionCapability.STDDEV_SAMP);
        // STDDEV_SAMP_DISTINCT
        cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE);
        cap.supportAggregateFunction(AggregateFunctionCapability.VARIANCE_DISTINCT);
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_POP);
        // VAR_POP_DISTINCT
        cap.supportAggregateFunction(AggregateFunctionCapability.VAR_SAMP);
        // VAR_SAMP_DISTINCT

        // Scalar functions
        cap.supportScalarFunction(ScalarFunctionCapability.CEIL);
        cap.supportScalarFunction(ScalarFunctionCapability.DIV);
        cap.supportScalarFunction(ScalarFunctionCapability.FLOOR);
        cap.supportScalarFunction(ScalarFunctionCapability.SIGN);
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
        // not supported cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_INSTR);
     // not supported cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_REPLACE);
     // not supported cap.supportScalarFunction(ScalarFunctionCapability.REGEXP_SUBSTR);
        cap.supportScalarFunction(ScalarFunctionCapability.REPEAT);
        cap.supportScalarFunction(ScalarFunctionCapability.REPLACE);
        // REVERSE is not supported
        cap.supportScalarFunction(ScalarFunctionCapability.RIGHT);
        cap.supportScalarFunction(ScalarFunctionCapability.RPAD);
        cap.supportScalarFunction(ScalarFunctionCapability.RTRIM);
        cap.supportScalarFunction(ScalarFunctionCapability.SOUNDEX);
        cap.supportScalarFunction(ScalarFunctionCapability.RPAD);
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
        // CONVERT_TZ is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_DATE);
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_TIMESTAMP);
        // DATE_TRUNC is not supported. Format options for TRUNCATE are different.
        // DAY is not supported. EXTRACT does not work on strings.
        // DAYS_BETWEEN is not supported. EXTRACT does not work on strings.
        // not supported cap.supportScalarFunction(ScalarFunctionCapability.DBTIMEZONE);
        // EXTRACT is not supported. SECOND must be cast to DOUBLE.
        // HOURS_BETWEEN is not supported. EXTRACT does not work on strings.
        cap.supportScalarFunction(ScalarFunctionCapability.LOCALTIMESTAMP);
        // MINUTE is not supported. EXTRACT does not work on strings.
        // MINUTES_BETWEEN is not supported. EXTRACT does not work on strings.
        // MONTH is not supported. EXTRACT does not work on strings.
        // MONTHS_BETWEEN is not supported. EXTRACT does not work on strings.
        //cap.supportScalarFunction(ScalarFunctionCapability.NUMTODSINTERVAL);
        //cap.supportScalarFunction(ScalarFunctionCapability.NUMTOYMINTERVAL);
        // POSIX_TIME is not supported. Does not work on strings.
        // SECOND is not supported. EXTRACT does not work on strings.
        // SECONDS_BETWEEN is not supported. EXTRACT does not work on strings.
        // SESSIONTIMEZONE is not supported
        cap.supportScalarFunction(ScalarFunctionCapability.SYSDATE);
        cap.supportScalarFunction(ScalarFunctionCapability.SYSTIMESTAMP);
        // WEEK is not supported.
        // YEAR is not supported. EXTRACT does not work on strings.
        // YEARS_BETWEEN is not supported. EXTRACT does not work on strings.
        // ST_X is not supported.
        // ST_Y is not supported.
        // ST_ENDPOINT is not supported.
        // ST_ISCLOSED is not supported.
        // ST_ISRING is not supported.
        // ST_LENGTH is not supported.
        // ST_NUMPOINTS is not supported.
        // ST_POINTN is not supported.
        // ST_STARTPOINT is not supported.
        // ST_AREA is not supported.
        // ST_EXTERIORRING is not supported.
        // ST_INTERIORRINGN is not supported.
        // ST_NUMINTERIORRINGS is not supported.
        // ST_GEOMETRYN is not supported.
        // ST_NUMGEOMETRIES is not supported.
        // ST_BOUNDARY is not supported.
        // ST_BUFFER is not supported.
        // ST_CENTROID is not supported.
        // ST_CONTAINS is not supported.
        // ST_CONVEXHULL is not supported.
        // ST_CROSSES is not supported.
        // ST_DIFFERENCE is not supported.
        // ST_DIMENSION is not supported.
        // ST_DISJOINT is not supported.
        // ST_DISTANCE is not supported.
        // ST_ENVELOPE is not supported.
        // ST_EQUALS is not supported.
        // ST_FORCE2D is not supported.
        // ST_GEOMETRYTYPE is not supported.
        // ST_INTERSECTION is not supported.
        // ST_INTERSECTS is not supported.
        // ST_ISEMPTY is not supported.
        // ST_ISSIMPLE is not supported.
        // ST_OVERLAPS is not supported.
        // ST_SETSRID is not supported.
        // ST_SYMDIFFERENCE is not supported.
        // ST_TOUCHES is not supported.
        // ST_TRANSFORM is not supported.
        // ST_UNION is not supported.
        // ST_WITHIN is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.CAST);
        // IS_NUMBER is not supported.
        // IS_BOOLEAN is not supported.
        // IS_DATE is not supported.
        // IS_DSINTERVAL is not supported.
        // IS_YMINTERVAL is not supported.
        // IS_TIMESTAMP is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.TO_CHAR);
        cap.supportScalarFunction(ScalarFunctionCapability.TO_DATE);
        cap.supportScalarFunction(ScalarFunctionCapability.TO_NUMBER);
        cap.supportScalarFunction(ScalarFunctionCapability.TO_TIMESTAMP);
        // BIT_CHECK is not supported.
        // BIT_NOT is not supported.
        // BIT_OR is not supported.
        // BIT_SET is not supported.
        // BIT_XOR is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.CASE);
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_SCHEMA);
        cap.supportScalarFunction(ScalarFunctionCapability.CURRENT_USER);
        cap.supportScalarFunction(ScalarFunctionCapability.NULLIFZERO);
        // SYS_GUID is not supported.
        cap.supportScalarFunction(ScalarFunctionCapability.ZEROIFNULL);

        return cap;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcCatalogs()
    {
        return SchemaOrCatalogSupport.UNSUPPORTED;
    }

    @Override
    public SchemaOrCatalogSupport supportsJdbcSchemas()
    {
        return SchemaOrCatalogSupport.SUPPORTED;
    }

    @Override
    public IdentifierCaseHandling getUnquotedIdentifierHandling()
    {
        return IdentifierCaseHandling.INTERPRET_AS_UPPER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling()
    {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }

    @Override
    public String applyQuote(String identifier)
    {
        // If identifier contains double quotation marks ", it needs to be espaced by another double quotation mark. E.g. "a""b" is the identifier a"b in the db.
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String applyQuoteIfNeeded(String identifier)
    {
        // Quoted identifiers can contain any unicode char except dot (.).
        // This is a simplified rule, which might cause that some identifiers are quoted although not needed
        boolean isSimpleIdentifier = identifier.matches("^[A-Z][0-9A-Z_]*");
        if (isSimpleIdentifier) {
            return identifier;
        } else {
            return applyQuote(identifier);
        }
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(
            SqlGenerationContext context)
    {
        //DB2 does not know catalogs
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(
            SqlGenerationContext context)
    {
        return true;
    }
    
    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(SqlGenerationContext context) {
        return new DB2SqlGenerationVisitor(this, context);
    }

    @Override
    public NullSorting getDefaultNullSorting()
    {
        //default db2 behaviour is to set nulls to the end of the result
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    public String getStringLiteral(String value)
    {
        // Don't forget to escape single quote
        return "'" + value.replace("'", "''") + "'";
    }
    
    @Override
    public DataType mapJdbcType(ResultSet cols) throws SQLException {
        DataType colType = null;
        int jdbcType = cols.getInt("DATA_TYPE");
        
        switch (jdbcType) {
        case Types.CLOB:
            colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
            break;
        case 1111:
            colType = DataType.createVarChar(DataType.maxExasolVarcharSize, DataType.ExaCharset.UTF8);
            break;
        // set timestamp to varchar as db2 offers a much higher precision than exasol
        // however java timestamp only supports nanoseconds -> varchar(32)
        case Types.TIMESTAMP:
            colType = DataType.createVarChar(32, DataType.ExaCharset.UTF8);
            break;
            
        // db2 driver always delivers UTF8 Characters no matter what encoding is specified for var + char data
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.LONGVARCHAR:
        case Types.CHAR:
        case Types.NCHAR:
        case Types.LONGNVARCHAR: {
            int size = cols.getInt("COLUMN_SIZE");
            DataType.ExaCharset charset =  DataType.ExaCharset.UTF8;
            if (size <= DataType.maxExasolVarcharSize) {
                colType = DataType.createVarChar(size, charset);
            } else {
                colType = DataType.createVarChar(DataType.maxExasolVarcharSize, charset);
            }
            break;
        }
            
        // VARCHAR  and CHAR for bit data -> will be converted to hex string so we have to double the size
        case -2:
            colType = DataType.createChar(cols.getInt("COLUMN_SIZE")*2, DataType.ExaCharset.ASCII);
        case -3:
            colType = DataType.createVarChar(cols.getInt("COLUMN_SIZE")*2, DataType.ExaCharset.ASCII);
            break;
        }
        return colType;
    }

}
