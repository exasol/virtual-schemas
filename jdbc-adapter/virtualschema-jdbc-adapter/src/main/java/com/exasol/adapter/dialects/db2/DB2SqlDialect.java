package com.exasol.adapter.dialects.db2;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.RemoteMetadataReader;

/**
 * Dialect for DB2 using the DB2 Connector JDBC driver.
 */
public class DB2SqlDialect extends AbstractSqlDialect {
    private static final String NAME = "DB2";
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY, DEBUG_ADDRESS_PROPERTY,
            LOG_LEVEL_PROPERTY);

    public DB2SqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new DB2MetadataReader(this.connection, this.properties);
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, this.remoteMetadataReader, this.connection);
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
                ADD_YEARS, CURRENT_DATE, CURRENT_TIMESTAMP, LOCALTIMESTAMP, SYSDATE, SYSTIMESTAMP, CAST, TO_CHAR,
                TO_DATE, TO_NUMBER, TO_TIMESTAMP, CASE, CURRENT_SCHEMA, CURRENT_USER, NULLIFZERO, ZEROIFNULL);
        return builder.build();
    }

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.NONE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    public String applyQuote(final String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
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
    public void validateProperties() throws PropertyValidationException {
        super.validateDialectName(getPublicName());
        super.validateProperties();
    }

    @Override
    protected List<String> getSupportedProperties() {
        return SUPPORTED_PROPERTIES;
    }
}