package com.exasol.adapter.dialects.impala;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.Connection;
import java.util.*;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.RemoteMetadataReader;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * Dialect for Impala, using the Cloudera Impala JDBC Driver/Connector (developed by Simba).
 * <p>
 * See http://www.cloudera.com/documentation/enterprise/latest/topics/impala_langref.html
 */
public class ImpalaSqlDialect extends AbstractSqlDialect {
    static final String NAME = "IMPALA";
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
            DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    @Override
    public Capabilities getCapabilities() {
        final Capabilities.Builder builder = Capabilities.builder();
        builder.addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING,
                ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET)
                .addLiteral(NULL, DOUBLE, EXACTNUMERIC, STRING, BOOL) //
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IN_CONSTLIST,
                        IS_NULL, IS_NOT_NULL) //
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, GROUP_CONCAT, GROUP_CONCAT_SEPARATOR, SUM,
                        SUM_DISTINCT, MIN, MAX, AVG) //
                .addScalarFunction(ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COSH, COT, DEGREES, EXP, FLOOR, GREATEST,
                        LEAST, LN, LOG, MOD, NEG, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SINH, SQRT, TAN, TANH, TRUNC,
                        BIT_AND, BIT_NOT, BIT_OR, BIT_XOR, BIT_SET, CAST, ADD_MONTHS, CURRENT_TIMESTAMP, DAY, ADD_DAYS,
                        ADD_HOURS, MINUTE, ADD_MINUTES, MONTH, MONTHS_BETWEEN, SECOND, ADD_SECONDS, TO_DATE,
                        TO_TIMESTAMP, ADD_WEEKS, YEAR, ADD_YEARS, ASCII, CONCAT, INSTR, LENGTH, LOCATE, LOWER, LPAD,
                        LTRIM, REGEXP_REPLACE, REPEAT, REVERSE, RPAD, RTRIM, SPACE, SUBSTR, TRANSLATE, TRIM, UPPER,
                        SYSDATE);
        return builder.build();
    }

    /**
     * Create a new instance of the {@link ImpalaSqlDialect}.
     *
     * @param connection JDBC connection
     * @param properties user-defined adapter properties
     */
    public ImpalaSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> aliases = new EnumMap<>(ScalarFunction.class);
        aliases.put(ScalarFunction.NEG, "NEGATIVE");
        aliases.put(ScalarFunction.TRUNC, "TRUNCATE");
        aliases.put(ScalarFunction.BIT_AND, "BITAND");
        aliases.put(ScalarFunction.BIT_NOT, "BITNOT");
        aliases.put(ScalarFunction.BIT_OR, "BITOR");
        aliases.put(ScalarFunction.BIT_XOR, "BITXOR");
        aliases.put(ScalarFunction.BIT_SET, "SETBIT");
        aliases.put(ScalarFunction.ADD_DAYS, "DAYS_ADD");
        aliases.put(ScalarFunction.MONTHS_BETWEEN, "INT_MONTHS_BETWEEN");
        aliases.put(ScalarFunction.ADD_MINUTES, "MINUTES_ADD");
        aliases.put(ScalarFunction.ADD_MONTHS, "MONTHS_ADD");
        aliases.put(ScalarFunction.ADD_SECONDS, "SECONDS_ADD");
        aliases.put(ScalarFunction.ADD_WEEKS, "WEEKS_ADD");
        aliases.put(ScalarFunction.ADD_YEARS, "YEARS_ADD");
        aliases.put(ScalarFunction.SYSDATE, "NOW");
        return aliases;
    }

    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.SINGLE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    public String applyQuote(final String identifier) {
        return "`" + identifier + "`";
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
        return new ImpalaSqlGenerationVisitor(this, context);
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_HIGH;
    }

    @Override
    protected List<String> getSupportedProperties() {
        return SUPPORTED_PROPERTIES;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new ImpalaMetadataReader(this.connection, this.properties);
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, this.remoteMetadataReader, this.connection);
    }
}