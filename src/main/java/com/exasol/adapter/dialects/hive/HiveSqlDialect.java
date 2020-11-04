package com.exasol.adapter.dialects.hive;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.adapter.dialects.hive.HiveProperties.HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY;

import java.sql.SQLException;
import java.util.*;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.ScalarFunction;
import com.exasol.adapter.sql.SqlNodeVisitor;

/**
 * Dialect for Hive, using the Cloudera Hive JDBC Driver/Connector (developed by Simba). Only supports Hive 2.1.0 and
 * later because of the order by (nulls first/last option).
 */
public class HiveSqlDialect extends AbstractSqlDialect {
    static final String NAME = "HIVE";
    private static final Capabilities CAPABILITIES = createCapabilityList();

    private static Capabilities createCapabilityList() {
        return Capabilities.builder()
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, JOIN,
                        JOIN_TYPE_INNER, JOIN_TYPE_LEFT_OUTER, JOIN_TYPE_RIGHT_OUTER, JOIN_TYPE_FULL_OUTER,
                        JOIN_CONDITION_EQUI)
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IN_CONSTLIST,
                        IS_NULL, IS_NOT_NULL)
                .addLiteral(NULL, BOOL, DATE, TIMESTAMP, DOUBLE, EXACTNUMERIC, STRING)
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
                        STDDEV_POP, STDDEV_POP_DISTINCT, STDDEV_SAMP, STDDEV_SAMP_DISTINCT, VAR_POP, VAR_POP_DISTINCT,
                        VAR_SAMP, VAR_SAMP_DISTINCT)
                .addScalarFunction(ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, CEIL, COS, DEGREES, DIV, EXP,
                        FLOOR, LN, LOG, MOD, POWER, RADIANS, SIGN, SIN, SQRT, TAN, ASCII, CONCAT, LENGTH, LOWER, LPAD,
                        REPEAT, REVERSE, RPAD, SOUNDEX, SPACE, SUBSTR, TRANSLATE, UPPER, ADD_DAYS, ADD_MONTHS,
                        CURRENT_DATE, CURRENT_TIMESTAMP, DATE_TRUNC, DAY, DAYS_BETWEEN, MINUTE, MONTH, MONTHS_BETWEEN,
                        SECOND, WEEK, CAST, BIT_AND, BIT_OR, BIT_XOR, CURRENT_USER) //
                .build();
    }

    /**
     * Create a new instance of the {@link HiveSqlDialect}.
     *
     * @param connectionFactory factory for the JDBC connection to the remoted data source
     * @param properties        user-defined adapter properties
     */
    public HiveSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties,
                Set.of(CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY));
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
    }

    /**
     * Quote from user manual The Cloudera JDBC Driver for Apache Hive supports both catalogs and schemas to make it
     * easy for the driver to work with various JDBC applications. Since Hive only organizes tables into
     * schemas/databases, the driver provides a synthetic catalog called “HIVE” under which all of the schemas/databases
     * are organized. The driver also maps the JDBC schema to the Hive schema/database.
     */
    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.SINGLE;
    }

    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
    }

    @Override
    // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
    public String applyQuote(final String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
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
        return NullSorting.NULLS_SORTED_LOW;
    }

    @Override
    public SqlNodeVisitor<String> getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new HiveSqlGenerationVisitor(this, context);
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> scalarAliases = new EnumMap<>(ScalarFunction.class);
        scalarAliases.put(ScalarFunction.ADD_DAYS, "DATE_ADD");
        scalarAliases.put(ScalarFunction.DAYS_BETWEEN, "DATEDIFF");
        scalarAliases.put(ScalarFunction.WEEK, "WEEKOFYEAR");
        scalarAliases.put(ScalarFunction.CURRENT_USER, "CURRENT_USER()");
        return scalarAliases;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new HiveMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(
                    "Unable to create Hive remote metadata reader. Caused by: " + exception.getMessage(), exception);
        }
    }

    @Override
    public void validateProperties() throws PropertyValidationException {
        super.validateProperties();
        validateCastNumberToDecimalProperty(HIVE_CAST_NUMBER_TO_DECIMAL_PROPERTY);
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
    }
}