package com.exasol.adapter.dialects.db2;

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
import com.exasol.adapter.sql.SqlNodeVisitor;

/**
 * Dialect for DB2 using the DB2 Connector JDBC driver.
 */
public class DB2SqlDialect extends AbstractSqlDialect {
    static final String NAME = "DB2";
    private static final Capabilities CAPABILITIES = createCapabilityList();

    /**
     * Create a new instance of the {@link DB2SqlDialect}.
     *
     * @param connectionFactory factory for the JDBC connection to the remote data source
     * @param properties        user-defined adapter properties
     */
    public DB2SqlDialect(final ConnectionFactory connectionFactory, final AdapterProperties properties) {
        super(connectionFactory, properties, Set.of(SCHEMA_NAME_PROPERTY));
    }

    @Override
    protected RemoteMetadataReader createRemoteMetadataReader() {
        try {
            return new DB2MetadataReader(this.connectionFactory.getConnection(), this.properties);
        } catch (final SQLException exception) {
            throw new RemoteMetadataReaderException(
                    "Unable to create DB2 remote metadata reader. Caused by: " + exception.getMessage(), exception);
        }
    }

    @Override
    protected QueryRewriter createQueryRewriter() {
        return new ImportIntoQueryRewriter(this, createRemoteMetadataReader(), this.connectionFactory);
    }

    @Override
    public String getName() {
        return NAME;
    }

    private static Capabilities createCapabilityList() {
        return Capabilities.builder()
                .addMain(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS, AGGREGATE_SINGLE_GROUP,
                        AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION, AGGREGATE_GROUP_BY_TUPLE,
                        AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT)
                .addPredicate(AND, OR, NOT, EQUAL, NOTEQUAL, LESS, LESSEQUAL, LIKE, LIKE_ESCAPE, BETWEEN, IN_CONSTLIST,
                        IS_NULL, IS_NOT_NULL)
                .addLiteral(NULL, DATE, TIMESTAMP, TIMESTAMP_UTC, DOUBLE, EXACTNUMERIC, STRING, INTERVAL)
                .addAggregateFunction(COUNT, COUNT_STAR, COUNT_DISTINCT, GROUP_CONCAT, GROUP_CONCAT_SEPARATOR,
                        GROUP_CONCAT_ORDER_BY, SUM, SUM_DISTINCT, MIN, MAX, AVG, AVG_DISTINCT, MEDIAN, FIRST_VALUE,
                        LAST_VALUE, STDDEV, STDDEV_POP, STDDEV_SAMP, VARIANCE, VARIANCE_DISTINCT, VAR_POP, VAR_SAMP)
                .addScalarFunction(CEIL, DIV, FLOOR, SIGN, ADD, SUB, MULT, FLOAT_DIV, NEG, ABS, ACOS, ASIN, ATAN, ATAN2,
                        COS, COSH, COT, DEGREES, EXP, GREATEST, LEAST, LN, LOG, MOD, POWER, RADIANS, SIN, SINH, SQRT,
                        TAN, TANH, ASCII, CHR, INSTR, LENGTH, LOCATE, LOWER, LPAD, LTRIM, REPEAT, REPLACE, RIGHT, RPAD,
                        RTRIM, SOUNDEX, SUBSTR, TRANSLATE, TRIM, UPPER, ADD_DAYS, ADD_HOURS, ADD_MINUTES, ADD_MONTHS,
                        ADD_SECONDS, ADD_WEEKS, ADD_YEARS, CURRENT_DATE, CURRENT_TIMESTAMP, LOCALTIMESTAMP, SYSDATE,
                        SYSTIMESTAMP, CAST, TO_CHAR, TO_DATE, TO_NUMBER, TO_TIMESTAMP, CASE, CURRENT_SCHEMA,
                        CURRENT_USER, NULLIFZERO, ZEROIFNULL) //
                .build();
    }

    /**
     * Get the DB2 dialect name.
     *
     * @return always "DB2"
     */
    public static String getPublicName() {
        return NAME;
    }

    @Override
    public Capabilities getCapabilities() {
        return CAPABILITIES;
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
    // https://www.ibm.com/support/knowledgecenter/SSEPGG_11.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0000720.html
    public String applyQuote(final String identifier) {
        return super.quoteIdentifierWithDoubleQuotes(identifier);
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
    public SqlNodeVisitor<String> getSqlGenerationVisitor(final SqlGenerationContext context) {
        return new DB2SqlGenerationVisitor(this, context);
    }

    @Override
    public NullSorting getDefaultNullSorting() {
        return NullSorting.NULLS_SORTED_AT_END;
    }

    @Override
    // https://www.ibm.com/support/knowledgecenter/SSEPGG_11.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0000731.html
    public String getStringLiteral(final String value) {
        return super.quoteLiteralStringWithSingleQuote(value);
    }
}
