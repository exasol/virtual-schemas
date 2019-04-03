package com.exasol.adapter.dialects.impl;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.*;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.sql.SqlNode;

import utils.SqlTestUtil;

class ExasolSqlDialectTest {
    @CsvSource({ "A1, \"A1\"", //
            "A_1, \"A_1\"", //
            "A,\"A\"", //
            "A_a_1, \"A_a_1\"", //
            "1, \"1\"", //
            "1a, \"1a\", \"1a\"", //
            "'a\"b', \"a\"\"b\"" })
    @ParameterizedTest
    void testApplyQuoteIfNeeded(final String identifier, final String expectedQuotingResult) {
        final ExasolSqlDialect dialect = new ExasolSqlDialect(/* DialectTestData.getExasolDialectContext() */); // FIXME:
                                                                                                                // broken
                                                                                                                // test
        assertThat(dialect.applyQuoteIfNeeded(identifier), equalTo(expectedQuotingResult));
    }

    @Test
    void testExasolSqlDialectSupportsAllCapabilities() {
        final ExasolSqlDialect dialect = new ExasolSqlDialect(/* DialectTestData.getExasolDialectContext() */); // FIXME:
                                                                                                                // broken
                                                                                                                // test
        final Capabilities capabilities = dialect.getCapabilities();
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
        final SqlDialect dialect = new ExasolSqlDialect(/* DialectTestData.getExasolDialectContext() */); // FIXME:
                                                                                                          // broken test
        final SqlGenerationVisitor generator = dialect.getSqlGenerationVisitor(context);
        final String actualSql = node.accept(generator);
        assertThat(SqlTestUtil.normalizeSql(actualSql), equalTo(SqlTestUtil.normalizeSql(expectedSql)));
    }

    @CsvSource({ "FALSE, FALSE, FALSE, JDBC", //
            "TRUE, FALSE, FALSE, LOCAL", //
            "FALSE, TRUE, FALSE, EXA", //
            "FALSE, FALSE, TRUE, ORA" })
    @ParameterizedTest
    void testGetImportTypeLocal(final String local, final String fromExasol, final String fromOracle,
            final String expectedImportType) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ExasolSqlDialect.LOCAL_IMPORT_PROPERTY, local);
        properties.put(ExasolSqlDialect.EXASOL_IMPORT_PROPERTY, fromExasol);
        properties.put(ExasolSqlDialect.ORACLE_IMPORT_PROPERTY, fromOracle);
        final ExasolSqlDialect dialect = new ExasolSqlDialect(null, properties);
        assertThat(dialect.getImportType().toString(), equalTo(expectedImportType));
    }
}