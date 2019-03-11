package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.metadata.DataType;

import java.sql.SQLException;
import java.sql.Types;

import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

/**
 * Dialect for DB2 using the DB2 Connector JDBC driver.
 *
 * @author Karl Griesser (fullref@gmail.com)
 */
public class DB2SqlDialect extends AbstractSqlDialect {
    private static final String NAME = "DB2";

    public DB2SqlDialect(final SqlDialectContext context) {
        super(context);
    }

    public static String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        builder.addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
              AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING,
              ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT);
        builder.addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN, IN_CONSTLIST,
              IS_NULL, IS_NOT_NULL);
        builder.addLiteral(NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL);
        builder.addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, GROUP_CONCAT, GROUP_CONCAT_SEPARATOR,
              GROUP_CONCAT_ORDER_BY, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT, MEDIAN, FIRST_VALUE, LAST_VALUE,
              STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_SAMP);
        builder.addScalarFunction(CEIL, DIV, FLOOR, SIGN, ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2,
              COS, COSH, COT, DEGREES, EXP, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, SIN, SINH, SQRT, TAN, TANH,
              ASCII, CHR, INSTR, LENGTH, LOCATE, LOWER, LPAD, LTRIM, REPEAT, REPLACE, RIGHT, RPAD, RTRIM, SOUNDEX,
              SUBSTR, TRANSLATE, TRIM, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS, ADD_SECONDS, ADD_WEEKS,
              ADD_YEARS, CURRENT_DATE, CURRENT_TIMESTAMP, LOCALTIMESTAMP, SYSDATE, SYSTIMESTAMP, CAST, TO_CHAR, TO_DATE,
              TO_NUMBER, TO_TIMESTAMP, CASE, CURRENT_SCHEMA, CURRENT_USER, NULLIFZERO, ZEROIFNULL);
        return builder.build();
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
    public IdentifierCaseHandling getUnquotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_AS_UPPER;
    }

    @Override
    public IdentifierCaseHandling getQuotedIdentifierHandling() {
        return IdentifierCaseHandling.INTERPRET_CASE_SENSITIVE;
    }

    @Override
    public String applyQuote(final String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    @Override
    public String applyQuoteIfNeeded(final String identifier) {
        return applyQuote(identifier);
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    public SqlGenerationVisitor getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new DB2SqlGenerationVisitor(this, context);
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    public String getStringLiteral(final String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    @Override
    public DataType dialectSpecificMapJdbcType(final JdbcTypeDescription jdbcTypeDescription) throws SQLException {
        DataType colType = null;
        final int jdbcType = jdbcTypeDescription.getJdbcType();

        switch (jdbcType) {
        case Types.CLOB:
            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            break;
        case 1111:
            colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, DataType.ExaCharset.UTF8);
            break;
        case Types.TIMESTAMP:
            colType = DataType.createVarChar(32, DataType.ExaCharset.UTF8);
            break;
        case Types.VARCHAR:
        case Types.NVARCHAR:
        case Types.LONGVARCHAR:
        case Types.CHAR:
        case Types.NCHAR:
        case Types.LONGNVARCHAR: {
            final int size = jdbcTypeDescription.getPrecisionOrSize();
            final DataType.ExaCharset charset = DataType.ExaCharset.UTF8;
            if (size <= DataType.MAX_EXASOL_VARCHAR_SIZE) {
                colType = DataType.createVarChar(size, charset);
            } else {
                colType = DataType.createVarChar(DataType.MAX_EXASOL_VARCHAR_SIZE, charset);
            }
            break;
        }
        case -2:
            colType = DataType.createChar(jdbcTypeDescription.getPrecisionOrSize() * 2, DataType.ExaCharset.ASCII);
            break;
        case -3:
            colType = DataType.createVarChar(jdbcTypeDescription.getPrecisionOrSize() * 2, DataType.ExaCharset.ASCII);
            break;
        }
        return colType;
    }
}
