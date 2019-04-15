package com.exasol.adapter.dialects.exasol;

import static com.exasol.reflect.ReflectionUtils.getMethodReturnViaReflection;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.SqlNode;

import utils.SqlTestUtil;

class ExasolSqlDialectTest {
    private ExasolSqlDialect dialect;

    @BeforeEach
    void beforeEach() {
        this.dialect = new ExasolSqlDialect(null, AdapterProperties.emptyProperties());
    }

    @CsvSource({ "A1, \"A1\"", //
            "A_1, \"A_1\"", //
            "A,\"A\"", //
            "A_a_1, \"A_a_1\"", //
            "1, \"1\"", //
            "1a, \"1a\", \"1a\"", //
            "'a\"b', \"a\"\"b\"" })
    @ParameterizedTest
    void testApplyQuoteIfNeeded(final String identifier, final String expectedQuotingResult) {
        assertThat(this.dialect.applyQuoteIfNeeded(identifier), equalTo(expectedQuotingResult));
    }

    @Test
    void testExasolSqlDialectSupportsAllCapabilities() {
        final Capabilities capabilities = this.dialect.getCapabilities();
        assertAll(() -> assertThat(capabilities.getMainCapabilities(), containsInAnyOrder(MainCapability.values())),
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
        final SqlGenerationContext context = new SqlGenerationContext("", schemaName, false, false);
        final SqlGenerationVisitor generator = this.dialect.getSqlGenerationVisitor(context);
        final String actualSql = node.accept(generator);
        assertThat(SqlTestUtil.normalizeSql(actualSql), equalTo(SqlTestUtil.normalizeSql(expectedSql)));
    }

    @CsvSource({ "FALSE, FALSE, JDBC", //
            "TRUE, FALSE, LOCAL", //
            "FALSE, TRUE, EXA" })
    @ParameterizedTest
    void testGetImportTypeLocal(final String local, final String fromExasol, final String expectedImportType) {
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(ExasolSqlDialect.LOCAL_IMPORT_PROPERTY, local);
        rawProperties.put(ExasolSqlDialect.EXASOL_IMPORT_PROPERTY, fromExasol);
        final ExasolSqlDialect dialect = new ExasolSqlDialect(null, new AdapterProperties(rawProperties));
        assertThat(dialect.getImportType().toString(), equalTo(expectedImportType));
    }

    @Test
    void testMetadataReaderClass() {
        assertThat(getMethodReturnViaReflection(this.dialect, "createRemoteMetadataReader"),
                instanceOf(ExasolMetadataReader.class));
    }
}