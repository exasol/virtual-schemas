package com.exasol.adapter.dialects.postgresql;

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
import com.exasol.adapter.sql.SqlNodeVisitor;

/**
 * This class implements the PostgreSQL dialect.
 */
public class PostgreSQLSqlDialect extends AbstractSqlDialect {
    static final String NAME = "POSTGRESQL";
    public static final String POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY = "POSTGRESQL_IDENTIFIER_MAPPING";
    private static final String POSTGRESQL_IDENTIFER_MAPPING_PRESERVE_ORIGINAL_CASE_VALUE = "PRESERVE_ORIGINAL_CASE";
    private static final String POSTGRESQL_IDENTIFIER_MAPPING_CONVERT_TO_UPPER_VALUE = "CONVERT_TO_UPPER";
    private static final PostgreSQLIdentifierMapping DEFAULT_POSTGRESS_IDENTIFIER_MAPPING = PostgreSQLIdentifierMapping.CONVERT_TO_UPPER;
    private static final Capabilities CAPABILITIES = createCapabilityList();
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
            IGNORE_ERRORS_PROPERTY, POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY, DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    private static Capabilities createCapabilityList() {
        return Capabilities.builder()
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET, JOIN,
                        JOIN_TYPE_INNER, JOIN_TYPE_LEFT_OUTER, JOIN_TYPE_RIGHT_OUTER, JOIN_TYPE_FULL_OUTER,
                        JOIN_CONDITION_EQUI)
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN, REGEXP_LIKE,
                        IN_CONSTLIST, IS_NULL, IS_NOT_NULL)
                .addLiteral(BOOL, NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING)
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
                        MEDIAN, FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT,
                        STDDEV_SAMP, STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT,
                        VAR_SAMP, VAR_SAMP_DISTINCT, GROUP_CONCAT)
                .addScalarFunction(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COSH, COT,
                        DEGREES, DIV, EXP, FLOOR, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, RAND, ROUND, SIGN, SIN,
                        SINH, SQRT, TAN, TANH, TRUNC, ASCII, BIT_LENGTH, CHR, CONCAT, INSTR, LENGTH, LOWER, LPAD, LTRIM,
                        OCTET_LENGTH, REGEXP_REPLACE, REPEAT, REPLACE, REVERSE, RIGHT, RPAD, RTRIM, SUBSTR, TRANSLATE,
                        TRIM, UNICODE, UNICODECHR, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS, ADD_SECONDS,
                        ADD_WEEKS, ADD_YEARS, SECONDS_BETWEEN, MINUTES_BETWEEN, HOURS_BETWEEN, DAYS_BETWEEN,
                        MONTHS_BETWEEN, YEARS_BETWEEN, MINUTE, SECOND, DAY, WEEK, MONTH, YEAR, CURRENT_DATE,
                        CURRENT_TIMESTAMP, DATE_TRUNC, EXTRACT, LOCALTIMESTAMP, POSIX_TIME, TO_CHAR, CASE, HASH_MD5)
                .build();
    }

    /**
     * Create a new instance of the {@link PostgreSQLSqlDialect}.
     *
     * @param connection JDBC connection
     * @param properties user-defined adapter properties
     */
    public PostgreSQLSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new PostgreSQLMetadataReader(this.connection, this.properties);
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, this.remoteMetadataReader, this.connection);
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
    }

    private PostgreSQLIdentifierMapping getIdentifierMapping() {
        if (this.properties.containsKey(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY)) {
            return PostgreSQLIdentifierMapping.parse(this.properties.get(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY));
        } else {
            return DEFAULT_POSTGRESS_IDENTIFIER_MAPPING;
        }
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.SUBSTR, "SUBSTRING");
        scalarAliases.put(ScalarFunction.HASH_MD5, "MD5");
        return scalarAliases;
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
        String postgreSQLIdentifier = identifier;
        if (getIdentifierMapping() != PostgreSQLIdentifierMapping.PRESERVE_ORIGINAL_CASE) {
            postgreSQLIdentifier = convertIdentifierToLowerCase(postgreSQLIdentifier);
        }
        return "\"" + postgreSQLIdentifier.replace("\"", "\"\"") + "\"";
    }

    private String convertIdentifierToLowerCase(final String identifier) {
        return identifier.toLowerCase();
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
        return new PostgresSQLSqlGenerationVisitor(this, context);
    }

    @Override
    public void validateProperties() throws PropertyValidationException {
        super.validateProperties();
        checkPostgreSQLIdentifierPropertyConsistency();
    }

    @Override
    protected List<String> getSupportedProperties() {
        return SUPPORTED_PROPERTIES;
    }

    private void checkPostgreSQLIdentifierPropertyConsistency() throws PropertyValidationException {
        if (this.properties.containsKey(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY)) {
            final String propertyValue = this.properties.get(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY);
            if (!propertyValue.equals(POSTGRESQL_IDENTIFER_MAPPING_PRESERVE_ORIGINAL_CASE_VALUE)
                    && !propertyValue.equals(POSTGRESQL_IDENTIFIER_MAPPING_CONVERT_TO_UPPER_VALUE)) {
                throw new PropertyValidationException("Value for " + POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY
                        + " must be " + POSTGRESQL_IDENTIFER_MAPPING_PRESERVE_ORIGINAL_CASE_VALUE + " or "
                        + POSTGRESQL_IDENTIFIER_MAPPING_CONVERT_TO_UPPER_VALUE);
            }
        }
    }
}