package com.exasol.adapter.dialects.impala;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
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
import com.exasol.adapter.sql.ScalarFunction;

/**
 * Dialect for Impala, using the Cloudera Impala JDBC Driver/Connector (developed by Simba).
 * <p>
 * See http://www.cloudera.com/documentation/enterprise/latest/topics/impala_langref.html
 */
public class ImpalaSqlDialect extends AbstractSqlDialect {
    static final String NAME = "IMPALA";

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
     * @param connectionFactory factory for the JDBC connection to the remote data source
     * @param properties        user-defined adapter properties
     */
    public ImpalaSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties, Set.of(CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY));
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
    // https://docs.cloudera.com/documentation/enterprise/latest/topics/impala_identifiers.html
    public String applyQuote(final String identifier) {
        return ImpalaIdentifier.of(identifier).quote();
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
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new ImpalaMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(
                    "Unable to create Impala remote metadata reader. Caused by: " + exception.getMessage(), exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new ImportIntoQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
    }

    @Override
    // https://docs.cloudera.com/documentation/enterprise/5-9-x/topics/impala_literals.html#string_literals
    public String getStringLiteral(final String value) {
        if (value == null) {
            return "NULL";
        } else {
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
        }
    }
}