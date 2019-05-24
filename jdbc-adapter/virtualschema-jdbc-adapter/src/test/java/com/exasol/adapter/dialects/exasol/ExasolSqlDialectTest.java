package com.exasol.adapter.dialects.exasol;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.IS_LOCAL_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_GROUP_BY_COLUMN;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_GROUP_BY_EXPRESSION;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_GROUP_BY_TUPLE;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_HAVING;
import static com.exasol.adapter.capabilities.MainCapability.AGGREGATE_SINGLE_GROUP;
import static com.exasol.adapter.capabilities.MainCapability.FILTER_EXPRESSIONS;
import static com.exasol.adapter.capabilities.MainCapability.JOIN;
import static com.exasol.adapter.capabilities.MainCapability.JOIN_CONDITION_EQUI;
import static com.exasol.adapter.capabilities.MainCapability.JOIN_TYPE_FULL_OUTER;
import static com.exasol.adapter.capabilities.MainCapability.JOIN_TYPE_INNER;
import static com.exasol.adapter.capabilities.MainCapability.JOIN_TYPE_LEFT_OUTER;
import static com.exasol.adapter.capabilities.MainCapability.JOIN_TYPE_RIGHT_OUTER;
import static com.exasol.adapter.capabilities.MainCapability.LIMIT;
import static com.exasol.adapter.capabilities.MainCapability.LIMIT_WITH_OFFSET;
import static com.exasol.adapter.capabilities.MainCapability.ORDER_BY_COLUMN;
import static com.exasol.adapter.capabilities.MainCapability.ORDER_BY_EXPRESSION;
import static com.exasol.adapter.capabilities.MainCapability.SELECTLIST_EXPRESSIONS;
import static com.exasol.adapter.capabilities.MainCapability.SELECTLIST_PROJECTION;
import static com.exasol.adapter.dialects.exasol.ExasolProperties.EXASOL_CONNECTION_STRING_PROPERTY;
import static com.exasol.adapter.dialects.exasol.ExasolProperties.EXASOL_IMPORT_PROPERTY;
import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.AggregateFunctionCapability;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.LiteralCapability;
import com.exasol.adapter.capabilities.PredicateCapability;
import com.exasol.adapter.capabilities.ScalarFunctionCapability;
import com.exasol.adapter.dialects.DialectTestData;
import com.exasol.adapter.dialects.PropertyValidationException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.sql.SqlNode;

import utils.SqlTestUtil;

class ExasolSqlDialectTest {
    private ExasolSqlDialect dialect;
    private Map<String, String> rawProperties;

    @BeforeEach
    void beforeEach() {
        this.dialect = new ExasolSqlDialect(null, AdapterProperties.emptyProperties());
        this.rawProperties = new HashMap<>();
    }

    @CsvSource({ "A1, \"A1\"", //
            "A_1, \"A_1\"", //
            "A,\"A\"", //
            "A_a_1, \"A_a_1\"", //
            "1, \"1\"", //
            "1a, \"1a\", \"1a\"", //
            "'a\"b', \"a\"\"b\"" })
    @ParameterizedTest
    void testApplyQuote(final String identifier, final String expectedQuotingResult) {
        assertThat(this.dialect.applyQuote(identifier), equalTo(expectedQuotingResult));
    }

    @Test
    void testExasolSqlDialectSupportsAllCapabilities() {
        final Capabilities capabilities = this.dialect.getCapabilities();
        assertAll(
                () -> assertThat(capabilities.getMainCapabilities(),
                        containsInAnyOrder(SELECTLIST_PROJECTION, SELECTLIST_EXPRESSIONS, FILTER_EXPRESSIONS,
                                AGGREGATE_SINGLE_GROUP, AGGREGATE_GROUP_BY_COLUMN, AGGREGATE_GROUP_BY_EXPRESSION,
                                AGGREGATE_GROUP_BY_TUPLE, AGGREGATE_HAVING, ORDER_BY_COLUMN, ORDER_BY_EXPRESSION, LIMIT,
                                LIMIT_WITH_OFFSET, JOIN, JOIN_TYPE_INNER, JOIN_TYPE_LEFT_OUTER, JOIN_TYPE_RIGHT_OUTER,
                                JOIN_TYPE_FULL_OUTER, JOIN_CONDITION_EQUI)),
                () -> assertThat(capabilities.getLiteralCapabilities(), containsInAnyOrder(LiteralCapability.values())),
                () -> assertThat(capabilities.getPredicateCapabilities(),
                        containsInAnyOrder(PredicateCapability.values())),
                () -> assertThat(capabilities.getScalarFunctionCapabilities(),
                        containsInAnyOrder(ScalarFunctionCapability.values())),
                () -> assertThat(capabilities.getAggregateFunctionCapabilities(),
                        containsInAnyOrder(AggregateFunctionCapability.values())));
    }

    @Test
    void testSqlGenerator() throws AdapterException {
        final SqlNode node = DialectTestData.getTestSqlNode();
        final String schemaName = "SCHEMA";
        final String expectedSql = "SELECT \"USER_ID\", COUNT(\"URL\") FROM \"" + schemaName + "\".\"CLICKS\""
                + " WHERE 1 < \"USER_ID\"" + " GROUP BY \"USER_ID\"" + " HAVING 1 < COUNT(\"URL\")"
                + " ORDER BY \"USER_ID\"" + " LIMIT 10";
        final SqlGenerationContext context = new SqlGenerationContext("", schemaName, false);
        final SqlGenerationVisitor generator = this.dialect.getSqlGenerationVisitor(context);
        final String actualSql = node.accept(generator);
        assertThat(SqlTestUtil.normalizeSql(actualSql), equalTo(SqlTestUtil.normalizeSql(expectedSql)));
    }

    @CsvSource({ "FALSE, FALSE, JDBC", //
            "TRUE, FALSE, LOCAL", //
            "FALSE, TRUE, EXA" })
    @ParameterizedTest
    void testGetImportTypeLocal(final String local, final String fromExasol, final String expectedImportType) {
        this.rawProperties.put(IS_LOCAL_PROPERTY, local);
        this.rawProperties.put(EXASOL_IMPORT_PROPERTY, fromExasol);
        final ExasolSqlDialect dialect = new ExasolSqlDialect(null, new AdapterProperties(this.rawProperties));
        assertThat(dialect.getImportType().toString(), equalTo(expectedImportType));
    }

    @Test
    void testMetadataReaderClass() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createRemoteMetadataReader"),
                instanceOf(ExasolMetadataReader.class));
    }

    @Test
    void testQueryRewriterClass() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createQueryRewriter"),
                instanceOf(ExasolQueryRewriter.class));
    }

    @Test
    void checkValidBoolOptions1() throws PropertyValidationException {
        setMandatoryProperties("EXASOL");
        this.rawProperties.put(EXASOL_IMPORT_PROPERTY, "TrUe");
        this.rawProperties.put(EXASOL_CONNECTION_STRING_PROPERTY, "localhost:5555");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ExasolSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void checkValidBoolOptions2() throws PropertyValidationException {
        setMandatoryProperties("EXASOL");
        this.rawProperties.put(EXASOL_IMPORT_PROPERTY, "FalSe");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ExasolSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testInconsistentExasolProperties() {
        setMandatoryProperties("EXASOL");
        this.rawProperties.put(EXASOL_CONNECTION_STRING_PROPERTY, "localhost:5555");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ExasolSqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(),
                containsString("You defined the property EXA_CONNECTION_STRING without setting IMPORT_FROM_EXA "));
    }

    @Test
    void testInvalidExasolProperties() {
        setMandatoryProperties("EXASOL");
        this.rawProperties.put(EXASOL_IMPORT_PROPERTY, "True");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ExasolSqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(),
                containsString("You defined the property IMPORT_FROM_EXA, please also define EXA_CONNECTION_STRING"));
    }

    @Test
    void testValidateDialectNameProperty() {
        setMandatoryProperties("ORACLE");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ExasolSqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(), containsString(
                "The dialect EXASOL cannot have the name ORACLE. You specified the wrong dialect name or created the wrong dialect class."));
    }

    @Test
    void testValidateCatalogProperty() throws PropertyValidationException {
        setMandatoryProperties("EXASOL");
        this.rawProperties.put(CATALOG_NAME_PROPERTY, "MY_CATALOG");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ExasolSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        setMandatoryProperties("EXASOL");
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ExasolSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void checkInvalidIsLocalProperty() {
        setMandatoryProperties("EXASOL");
        this.rawProperties.put(IS_LOCAL_PROPERTY, "asdasd");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ExasolSqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(), containsString(
                "The value 'asdasd' for the property IS_LOCAL is invalid. It has to be either 'true' or 'false' (case "
                        + "insensitive)"));
    }

    @Test
    void checkValidIsLocalProperty1() throws PropertyValidationException {
        setMandatoryProperties("EXASOL");
        this.rawProperties.put(IS_LOCAL_PROPERTY, "TrUe");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ExasolSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void checkValidIsLocalProperty() throws PropertyValidationException {
        setMandatoryProperties("EXASOL");
        this.rawProperties.put(IS_LOCAL_PROPERTY, "FalSe");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new ExasolSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    private void setMandatoryProperties(final String sqlDialectProperty) {
        this.rawProperties.put(AdapterProperties.SQL_DIALECT_PROPERTY, sqlDialectProperty);
        this.rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, "MY_CONN");
    }
}