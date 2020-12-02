package com.exasol.adapter.dialects.bigquery;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ST_INTERSECTION;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ST_UNION;
import static com.exasol.adapter.dialects.bigquery.BigQueryProperties.BIGQUERY_ENABLE_IMPORT_PROPERTY;

import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.AggregateFunction;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * This class implements the SQL dialect of Google's Big Query.
 *
 * @see <a href="https://cloud.google.com/bigquery/">BigQuery</a>
 */
public class BigQuerySqlDialect extends AbstractSqlDialect {
    private static final Logger LOGGER = Logger.getLogger(BigQuerySqlDialect.class.getName());
    static final String NAME = "BIGQUERY";
    private static final Capabilities CAPABILITIES = createCapabilityList();

    /**
     * Create a new instance of the {@link BigQuerySqlDialect}.
     *
     * @param connectionFactory factory for the JDBC connection to the Big Query service
     * @param properties        user-defined adapter properties
     */
    public BigQuerySqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties,
                Set.of(CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, BIGQUERY_ENABLE_IMPORT_PROPERTY));
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new BigQueryMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(
                    "Unable to create BigQuery remote metadata reader. Caused by: " + exception.getMessage(),
                    exception);
        }

    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        if (this.properties.containsKey(BIGQUERY_ENABLE_IMPORT_PROPERTY)
                && "true".equalsIgnoreCase(this.properties.get(BIGQUERY_ENABLE_IMPORT_PROPERTY))) {
            LOGGER.warning("Attention: IMPORT is activated for the BIGQUERY dialect. "
                    + "Please be aware that using IMPORT with this dialect requires disabling important security features and is therefore not recommended!");
            return new ImportIntoQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
        } else {
            return new BigQueryQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    private static Capabilities createCapabilityList() {
        return Capabilities //
                .builder() //
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_EXPRESSION,
                        LIMIT, LIMIT_WITH_OFFSET) //
                .addLiteral(NULL, BOOL, DATE, TIMESTAMP, EXACTNUMERIC, STRING) //
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IN_CONSTLIST,
                        IS_NULL, IS_NOT_NULL) //
                .addScalarFunction(ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COSH, DEGREES, DIV, EXP, FLOOR, GREATEST,
                        LEAST, LN, LOG, MOD, POWER, RAND, ROUND, SIGN, SIN, SINH, SQRT, TAN, TANH, TRUNC,
                        COLOGNE_PHONETIC, CONCAT, INSERT, INSTR, LENGTH, LOWER, LPAD, LTRIM, REGEXP_REPLACE, REPEAT,
                        REPLACE, REVERSE, RIGHT, RPAD, RTRIM, SOUNDEX, SPACE, SUBSTR, TRIM, UPPER, CURRENT_DATE,
                        CURRENT_TIMESTAMP, DATE_TRUNC, DAY, EXTRACT, MINUTE, MONTH, SECOND, WEEK, YEAR, ST_X, ST_Y,
                        ST_LENGTH, ST_NUMPOINTS, ST_AREA, ST_BOUNDARY, ST_CENTROID, ST_CONTAINS, ST_DIFFERENCE,
                        ST_DIMENSION, ST_DISJOINT, ST_DISTANCE, ST_EQUALS, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY,
                        ST_TOUCHES, ST_UNION, ST_WITHIN, CAST, TO_TIMESTAMP, BIT_AND, BIT_OR, BIT_XOR, CASE, HASH_MD5) //
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT,
                        FIRST_VALUE, LAST_VALUE, STDDEV, STDDEV_DISTINCT, STDDEV_POP, STDDEV_POP_DISTINCT, STDDEV_SAMP,
                        STDDEV_SAMP_DISTINCT, VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_POP_DISTINCT, VAR_SAMP,
                        VAR_SAMP_DISTINCT, GROUP_CONCAT, APPROXIMATE_COUNT_DISTINCT) //
                .build();
    }

    @Override
    public Map<AggregateFunction, String> getAggregateFunctionAliases() {
        final Map<AggregateFunction, String> aliases = new EnumMap<>(AggregateFunction.class);
        aliases.put(AggregateFunction.APPROXIMATE_COUNT_DISTINCT, "APPROX_COUNT_DISTINCT");
        return aliases;
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> aliases = new EnumMap<>(ScalarFunction.class);
        aliases.put(ScalarFunction.HASH_MD5, "MD5");
        return aliases;
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
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
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
    public String applyQuote(final String identifier) {
        return "`" + identifier.replace("`", "\\`") + "`";
    }

    @Override
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return true;
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
    public void validateProperties() throws PropertyValidationException {
        super.validateProperties();
        validateBooleanProperty(BIGQUERY_ENABLE_IMPORT_PROPERTY);
    }

    @Override
    // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
    public String getStringLiteral(final String value) {
        if (value == null) {
            return "NULL";
        } else {
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
        }
    }
}