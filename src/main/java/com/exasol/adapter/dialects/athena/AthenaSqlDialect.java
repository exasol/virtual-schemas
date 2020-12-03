package com.exasol.adapter.dialects.athena;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.SQLException;
import java.util.Set;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;

/**
 * This class implements the SQL dialect of Amazon's AWS Athena.
 *
 * @see <a href="https://aws.amazon.com/athena/">AWS Athena</a>
 */
public class AthenaSqlDialect extends AbstractSqlDialect {
    static final String NAME = "ATHENA";
    private static final Capabilities CAPABILITIES = createCapabilityList();

    private static Capabilities createCapabilityList() {
        return Capabilities //
                .builder() //
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT) //
                .addLiteral(NULL, BOOL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL) //
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IS_NULL,
                        IS_NOT_NULL) //
                .addScalarFunction(CAST, ABS, CEIL, ACOS, ASIN, ATAN, ATAN2, COS, COSH, DEGREES, EXP, FLOOR, LN, LOG,
                        MOD, POWER, RADIANS, RAND, ROUND, SIGN, SIN, SQRT, TAN, TANH, TRUNC, BIT_AND, BIT_NOT, BIT_OR,
                        BIT_XOR, CHR, CONCAT, LENGTH, LOWER, LPAD, LTRIM, REPLACE, REVERSE, RPAD, RTRIM, SUBSTR, TRIM,
                        UPPER, CURRENT_DATE, CURRENT_TIMESTAMP, DATE_TRUNC, MINUTE, SECOND, DAY, MONTH, WEEK, YEAR,
                        REGEXP_REPLACE, HASH_MD5, HASH_SHA1) //
                .addAggregateFunction(COUNT, COUNT_STAR, SUM, MIN, MAX, AVG, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE,
                        VAR_POP, VAR_SAMP, APPROXIMATE_COUNT_DISTINCT) //
                .build();
    }

    /**
     * Create a new instance of the {@link AthenaSqlDialect}.
     *
     * @param connectionFactory factory for the JDBC connection to the remote data source
     * @param properties        user-defined adapter properties
     */
    public AthenaSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties, Set.of(CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY));
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
     * Get the type of support Athena has for catalogs.
     * <p>
     * While Athena itself does not use catalogs, the JDBC driver simulates a single catalog for a better compatibility
     * with standard products like BI tools.
     * <p>
     *
     * @return always {@link com.exasol.adapter.dialects.SqlDialect.StructureElementSupport#SINGLE}
     *
     * @see <a href=
     *      "https://s3.amazonaws.com/athena-downloads/drivers/JDBC/SimbaAthenaJDBC_2.0.7/docs/Simba+Athena+JDBC+Driver+Install+and+Configuration+Guide.pdf">
     *      Simba Athena JDBC Driver Install and Configuration Guide, section "Catalog and Schema Support"</a>
     */
    @Override
    public StructureElementSupport supportsJdbcCatalogs() {
        return StructureElementSupport.SINGLE;
    }

    /**
     * Get the type of support Athena has for schemas.
     * <p>
     * Athena knows schemas as a mirror of databases.
     *
     * @return always {@link com.exasol.adapter.dialects.SqlDialect.StructureElementSupport#MULTIPLE}
     *
     * @see <a href= "https://docs.aws.amazon.com/athena/latest/ug/show-databases.html">SHOW DATABASES documentation</a>
     */
    @Override
    public StructureElementSupport supportsJdbcSchemas() {
        return StructureElementSupport.MULTIPLE;
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
    // https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html
    public String applyQuote(final String identifier) {
        return AthenaIdentifier.of(identifier).quote();
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    // https://docs.aws.amazon.com/athena/latest/ug/select.html
    public String getStringLiteral(final String value) {
        return super.quoteLiteralStringWithSingleQuote(value);
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new AthenaMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(
                    "Unable to create Athena remote metadata reader. Caused by: " + exception.getMessage(), exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new ImportIntoQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
    }
}