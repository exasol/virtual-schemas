package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.Capabilities;
import com.exasol.adapter.capabilities.PredicateCapability;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.utils.SqlTestUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExasolSqlDialectTest {

    @Test
    public void testApplyQuoteIfNeeded() {
        ExasolSqlDialect dialect = new ExasolSqlDialect(DialectTestData.getExasolDialectContext());
        // Regular Identifiers
        assertEquals("A1", dialect.applyQuoteIfNeeded("A1"));
        assertEquals("A_1", dialect.applyQuoteIfNeeded("A_1"));
        assertEquals("A", dialect.applyQuoteIfNeeded("A"));
        
        // Irregular Identifiers
        assertEquals("\"A_a_1\"", dialect.applyQuoteIfNeeded("A_a_1"));
        assertEquals("\"1\"", dialect.applyQuoteIfNeeded("1"));
        assertEquals("\"1a\"", dialect.applyQuoteIfNeeded("1a"));
        assertEquals("\"a\"\"b\"", dialect.applyQuoteIfNeeded("a\"b"));
    }

    @Test
    public void testCapabilities() {
        // Test if EXASOL dialect really has all capabilities
        ExasolSqlDialect dialect = new ExasolSqlDialect(DialectTestData.getExasolDialectContext());
        Capabilities caps = dialect.getCapabilities();
        assertEquals(PredicateCapability.values().length, caps.getPredicateCapabilities().size());
    }
    
    @Test
    public void testSqlGenerator() throws AdapterException {
        SqlNode node = DialectTestData.getTestSqlNode();
        String schemaName = "SCHEMA";
        String expectedSql = "SELECT USER_ID, COUNT(URL) FROM " + schemaName + ".CLICKS" +
        " WHERE 1 < USER_ID" +
        " GROUP BY USER_ID" +
        " HAVING 1 < COUNT(URL)" +
        " ORDER BY USER_ID" +
        " LIMIT 10";
        SqlGenerationContext context = new SqlGenerationContext("", schemaName, false);
        SqlDialect dialect = new ExasolSqlDialect(DialectTestData.getExasolDialectContext());
        SqlGenerationVisitor generator = dialect.getSqlGenerationVisitor(context);
        String actualSql = node.accept(generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }

}
