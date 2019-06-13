package com.exasol.adapter.dialects.saphana;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.*;
import static com.exasol.adapter.capabilities.AggregateFunctionCapability.VAR_SAMP;
import static com.exasol.adapter.capabilities.LiteralCapability.*;
import static com.exasol.adapter.capabilities.MainCapability.*;
import static com.exasol.adapter.capabilities.PredicateCapability.*;
import static com.exasol.adapter.capabilities.ScalarFunctionCapability.*;

import java.sql.*;
import java.util.*;

import com.exasol.adapter.*;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.*;
import com.exasol.adapter.sql.*;

/**
 * This class implement the SAP HANA SQL dialect.
 *
 * @see <a href=
 *      "https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.03/en-US/f327b70cae564c53a766367a8aad0164.html">SAP
 *      HANA SQL</a>
 */
public class SapHanaSqlDialect extends AbstractSqlDialect {
    private static final String NAME = "SAPHANA";
    private static final Capabilities CAPABILITIES = createCapabilityList();
    private static final List<String> SUPPORTED_PROPERTIES = Arrays.asList(SQL_DIALECT_PROPERTY,
            CONNECTION_NAME_PROPERTY, CONNECTION_STRING_PROPERTY, USERNAME_PROPERTY, PASSWORD_PROPERTY,
            CATALOG_NAME_PROPERTY, SCHEMA_NAME_PROPERTY, TABLE_FILTER_PROPERTY, EXCLUDED_CAPABILITIES_PROPERTY,
            DEBUG_ADDRESS_PROPERTY, LOG_LEVEL_PROPERTY);

    /**
     * Get the Sap Hana dialect name
     *
     * @return always "SAPHANA"
     */
    public static String getPublicName() {
        return NAME;
    }

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
     * @param connection JDBC connection to the Sap Hana
     * @param properties user-defined adapter properties
     */
    public SapHanaSqlDialect(final Connection connection, final AdapterProperties properties) {
        super(connection, properties);
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
        return false;
    }

    @Override
    public String applyQuote(final String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_START;
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        return new BaseRemoteMetadataReader(this.connection, this.properties);
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new BaseQueryRewriter(this, this.remoteMetadataReader, this.connection);
    }

    @Override
    protected List<String> getSupportedProperties() {
        return SUPPORTED_PROPERTIES;
    }
}
