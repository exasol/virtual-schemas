package com.exasol.adapter.dialects.redshift;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.SQLException;
import java.util.*;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.*;

/**
 * This class implements the Redshift SQL dialect.
 */
public class RedshiftSqlDialect extends AbstractSqlDialect {
    static final String NAME = "REDSHIFT";
    private static final Capabilities CAPABILITIES = createCapabilityList();
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
            DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    private static Capabilities createCapabilityList() {
        return Capabilities.builder()
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET)
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN, REGEXP_LIKE,
                        IN_CONSTLIST, IS_NULL, IS_NOT_NULL)
                .addLiteral(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL)
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
                        MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT,
                        STDDEV_SAMP, STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT,
                        VAR_SAMP, VAR_SAMP_DISTINCT, GROUP_CONCAT)
                .addScalarFunction(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COT,
                        DEGREES, DIV, EXP, FLOOR, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, ROUND, SIGN, SIN, SINH,
                        SQRT, TAN, TANH, TRUNC, ASCII, CHR, CONCAT, LOCATE, INSTR, LENGTH, LOWER, LPAD, LTRIM,
                        REGEXP_INSTR, REGEXP_REPLACE, REGEXP_SUBSTR, REPEAT, REPLACE, REVERSE, RIGHT, RPAD, RTRIM,
                        SUBSTR, TRANSLATE, TRIM, UPPER, BIT_AND, BIT_OR, ADD_MONTHS, MONTHS_BETWEEN, CONVERT_TZ,
                        SYSDATE, YEAR, CURRENT_DATE, CURRENT_TIMESTAMP, EXTRACT, CAST, TO_NUMBER, TO_TIMESTAMP, TO_DATE,
                        HASH_SHA1, HASH_MD5, CURRENT_SCHEMA, CURRENT_USER)
                .build();
    }

    /**
     * Create a new instance of the {@link RedshiftSqlDialect}.
     *
     * @param connectionFactory factory for the JDBC connection to the remote data source
     * @param properties        user-defined adapter properties
     */
    public RedshiftSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.YEAR, "DATE_PART_YEAR");
        scalarAliases.put(ScalarFunction.CONVERT_TZ, "CONVERT_TIMEZONE");
        scalarAliases.put(ScalarFunction.HASH_MD5, "MD5");
        scalarAliases.put(ScalarFunction.HASH_SHA1, "FUNC_SHA1");
        scalarAliases.put(ScalarFunction.SUBSTR, "SUBSTRING");
        return scalarAliases;
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        return new EnumMap<>(AggregateFunction.class);
    }

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.MULTIPLE;
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
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    public SqlNodeVisitor<String> getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new RedshiftSqlGenerationVisitor(this, context);
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new RedshiftMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException("Unable to create Redshift remote metadata reader.", exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
    }

    @Override
    protected List<String> getSupportedProperties() {
        return SUPPORTED_PROPERTIES;
    }
}