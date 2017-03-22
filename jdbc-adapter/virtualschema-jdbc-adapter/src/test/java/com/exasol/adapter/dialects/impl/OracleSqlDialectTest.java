package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlGenerationContext;
import com.exasol.adapter.dialects.SqlGenerationVisitor;
import com.exasol.adapter.jdbc.SchemaAdapterNotes;
import com.exasol.adapter.sql.SqlNode;
import com.exasol.adapter.sql.SqlSelectList;
import com.exasol.adapter.sql.SqlStatementSelect;
import com.exasol.utils.SqlTestUtil;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;


public class OracleSqlDialectTest {

    @Test
    public void testSqlGeneratorWithLimit() throws AdapterException {
        SqlNode node = DialectTestData.getTestSqlNode();
        String schemaName = "SCHEMA";
        String expectedSql = "SELECT LIMIT_SUBSELECT.* FROM ( " +
                "  SELECT USER_ID, COUNT(URL) " +
                "    FROM SCHEMA.CLICKS" +
                "    WHERE 1 < USER_ID" +
                "    GROUP BY USER_ID" +
                "    HAVING 1 < COUNT(URL)" +
                "    ORDER BY USER_ID " +
                ") LIMIT_SUBSELECT WHERE ROWNUM <= 10" ;
        SqlGenerationContext context = new SqlGenerationContext("", schemaName, false);
        SqlDialectContext dialectContext = new SqlDialectContext(Mockito.mock(SchemaAdapterNotes.class));
        SqlDialect dialect = new OracleSqlDialect(dialectContext);
        SqlGenerationVisitor generator = dialect.getSqlGenerationVisitor(context);
        String actualSql = node.accept(generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }

    @Test
    public void testSqlGeneratorWithLimitOffset() throws AdapterException {
        SqlNode node = DialectTestData.getTestSqlNode();
        ((SqlStatementSelect)node).getLimit().setOffset(5);
        String schemaName = "SCHEMA";
        String expectedSql = "SELECT c0, c1 FROM (" +
                "  SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( " +
                "    SELECT USER_ID AS c0, COUNT(URL) AS c1 " +
                "      FROM SCHEMA.CLICKS" +
                "      WHERE 1 < USER_ID" +
                "      GROUP BY USER_ID" +
                "      HAVING 1 < COUNT(URL)" +
                "      ORDER BY USER_ID" +
                "  ) LIMIT_SUBSELECT WHERE ROWNUM <= 15 " +
                ") WHERE ROWNUM_SUB > 5";
        SqlGenerationContext context = new SqlGenerationContext("", schemaName, false);
        SqlDialectContext dialectContext = new SqlDialectContext(Mockito.mock(SchemaAdapterNotes.class));
        SqlDialect dialect = new OracleSqlDialect(dialectContext);
        SqlGenerationVisitor generator = dialect.getSqlGenerationVisitor(context);
        String actualSql = node.accept(generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }

    @Test
    public void testSqlGeneratorWithSelectStarAndOffset() throws AdapterException {
        SqlStatementSelect node = (SqlStatementSelect) DialectTestData.getTestSqlNode();
        node.getLimit().setOffset(5);
        node = new SqlStatementSelect(node.getFromClause(), SqlSelectList.createSelectStarSelectList(), node.getWhereClause(), node.getGroupBy(), node.getHaving(), node.getOrderBy(), node.getLimit());
        String schemaName = "SCHEMA";
        String expectedSql = "SELECT c0, c1 FROM (" +
                "  SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( " +
                "    SELECT USER_ID AS c0, URL AS c1 " +
                "      FROM SCHEMA.CLICKS" +
                "      WHERE 1 < USER_ID" +
                "      GROUP BY USER_ID" +
                "      HAVING 1 < COUNT(URL)" +
                "      ORDER BY USER_ID" +
                "  ) LIMIT_SUBSELECT WHERE ROWNUM <= 15 " +
                ") WHERE ROWNUM_SUB > 5";
        SqlGenerationContext context = new SqlGenerationContext("", schemaName, false);
        SqlDialectContext dialectContext = new SqlDialectContext(Mockito.mock(SchemaAdapterNotes.class));
        SqlDialect dialect = new OracleSqlDialect(dialectContext);
        SqlGenerationVisitor generator = dialect.getSqlGenerationVisitor(context);
        String actualSql = node.accept(generator);
        assertEquals(SqlTestUtil.normalizeSql(expectedSql), SqlTestUtil.normalizeSql(actualSql));
    }
}
