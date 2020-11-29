package com.exasol.adapter.dialects.saphana;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ST_INTERSECTION;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.ST_UNION;

import java.sql.SQLException;
import java.util.*;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.ScalarFunction;

/**
 * This class implements the SAP HANA SQL dialect.
 *
 * @see <a href= "https://help.sap.com/viewer/product/SAP_HANA_PLATFORM/2.0.04/en-US">SAP HANA SQL</a>
 */
public class SapHanaSqlDialect extends AbstractSqlDialect {
    static final String NAME = "SAPHANA";
    private static final Capabilities CAPABILITIES = createCapabilityList();

    private static Capabilities createCapabilityList() {
        return Capabilities //
                .builder() //
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT, LIMIT_WITH_OFFSET) //
                .addLiteral(NULL, BOOL, DATE, TIMESTAMP, DOUBLE, EXACTNUMERIC, STRING) //
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, REGEXP_LIKE, BETWEEN, IS_NULL,
                        IS_NOT_NULL) //
                .addScalarFunction(ABS, ACOS, ASIN, ATAN, ATAN2, CEIL, COS, COSH, COT, FLOOR, GREATEST, LEAST, LN, LOG,
                        MOD, POWER, RAND, ROUND, SIGN, SIN, SINH, SQRT, TAN, TANH, ASCII, CONCAT, LENGTH, LOCATE, LOWER,
                        LPAD, LTRIM, REGEXP_REPLACE, REGEXP_SUBSTR, RIGHT, RPAD, RTRIM, SOUNDEX, SUBSTR, TRIM, UNICODE,
                        UPPER, ADD_DAYS, ADD_MONTHS, ADD_SECONDS, ADD_YEARS, CURRENT_DATE, CURRENT_TIMESTAMP,
                        DAYS_BETWEEN, EXTRACT, MINUTE, MONTH, MONTHS_BETWEEN, SECOND, SECONDS_BETWEEN, WEEK, YEAR,
                        YEARS_BETWEEN, ST_X, ST_Y, ST_ENDPOINT, ST_ISCLOSED, ST_ISRING, ST_LENGTH, ST_NUMPOINTS,
                        ST_POINTN, ST_STARTPOINT, ST_AREA, ST_EXTERIORRING, ST_INTERIORRINGN, ST_NUMINTERIORRINGS,
                        ST_GEOMETRYN, ST_NUMGEOMETRIES, ST_BOUNDARY, ST_BUFFER, ST_CENTROID, ST_CONTAINS, ST_CONVEXHULL,
                        ST_CROSSES, ST_DIFFERENCE, ST_DIMENSION, ST_DISJOINT, ST_DISTANCE, ST_ENVELOPE, ST_EQUALS,
                        ST_GEOMETRYTYPE, ST_INTERSECTION, ST_INTERSECTS, ST_ISEMPTY, ST_ISSIMPLE, ST_OVERLAPS,
                        ST_SYMDIFFERENCE, ST_TOUCHES, ST_TRANSFORM, ST_UNION, ST_WITHIN, CAST, TO_DATE, TO_TIMESTAMP,
                        BIT_AND, BIT_NOT, BIT_OR, BIT_SET, BIT_XOR, CURRENT_SCHEMA, CURRENT_USER, HASH_MD5, HASH_SHA256) //
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, SUM, MIN, MAX, AVG, MEDIAN, FIRST_VALUE,
                        LAST_VALUE, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP) //
                .build();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Map<ScalarFunction, String> getScalarFunctionAliases() {
        final Map<ScalarFunction, String> aliases = new EnumMap<>(ScalarFunction.class);
        aliases.put(ScalarFunction.REGEXP_SUBSTR, "SUBSTRING_REGEXPR");
        aliases.put(ScalarFunction.REGEXP_REPLACE, "REPLACE_REGEXPR");
        aliases.put(ScalarFunction.SUBSTR, "SUBSTRING");
        aliases.put(ScalarFunction.BIT_AND, "BITAND");
        aliases.put(ScalarFunction.BIT_NOT, "BITNOT");
        aliases.put(ScalarFunction.BIT_OR, "BITOR");
        aliases.put(ScalarFunction.BIT_SET, "BITSET");
        aliases.put(ScalarFunction.BIT_XOR, "BITXOR");
        return aliases;
    }

    /**
     * Create a new instance of the {@link SapHanaSqlDialect}
     *
     * @param connectionFactory factory for the JDBC connection to the remote data source
     * @param properties        user-defined adapter properties
     */
    public SapHanaSqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties, Set.of(CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY));
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
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
    public boolean requiresCatalogQualifiedTableNames(final SqlGenerationContext context) {
        return false;
    }

    @Override
    public boolean requiresSchemaQualifiedTableNames(final SqlGenerationContext context) {
        return true;
    }

    @Override
    // http://sap.optimieren.de/hana/hana/html/_bsql_introduction.html
    public String applyQuote(final String identifier) {
        return super.quoteIdentifierWithDoubleQuotes(identifier);
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_START;
    }

    @Override
    // https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/LATEST/en-US/209f5020751910148fd8fe88aa4d79d9.html
    public String getStringLiteral(final String value) {
        return super.quoteLiteralStringWithSingleQuote(value);
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new SapHanaMetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(
                    "Unable to create HANA remote metadata reader. Caused by: " + exception.getMessage(), exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
    }
    @Override
    public SqlNodeVisitor<String> getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new SapHanaSqlGenerationVisitor(this, context);
    }
}
