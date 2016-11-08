package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tested with Oracle 12
 *
 * TODO Add tests for data types
 * TODO Test Expanding of SELECT * if elements of select list require casting
 */
public class OracleSqlDialectIT extends AbstractIntegrationTest {

    private static final String VIRTUAL_SCHEMA = "VS_ORACLE";
    private static final String ORACLE_SCHEMA = "C##LOADER";
    private static final boolean IS_LOCAL = false;

    @Before
    public void beforeMethod() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().oracleTestsRequested());
        setConnection(connectToExa());
        createOracleJDBCAdapter();
        createVirtualSchema(
                VIRTUAL_SCHEMA,
                OracleSqlDialect.NAME,
                "", ORACLE_SCHEMA,
                "",
                "C##LOADER",
                "loader",
                "ADAPTER.JDBC_ADAPTER",
                getConfig().getOracleJdbcConnectionString(),
                IS_LOCAL,
                getConfig().debugAddress(),
                "ALL_TYPES");
    }

    @Test
    public void testVirtualSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        ResultSet result = executeQuery("SELECT C3 FROM ALL_TYPES");
        result.next();
        assertEquals("cccccccccccccccccccc", result.getString(1));
    }

    @Test
    public void testSelectProjection() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C7 FROM ALL_TYPES";
        ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("12345.12345"));
        matchNextRow(result, new BigDecimal("12355.12345"));
        matchSingleRowExplain(query, "SELECT C7 FROM \"C##LOADER\".ALL_TYPES");
    }

    @Test
    public void testSelectExpression() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C7 + 1 FROM ALL_TYPES";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "12346.12345");
        matchNextRow(result, "12356.12345");
        matchSingleRowExplain(query, "SELECT CAST((C7 + 1) AS FLOAT) FROM \"C##LOADER\".ALL_TYPES");
    }

    @Test
    public void testFilterExpression() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C7 FROM ALL_TYPES WHERE C7 > 12346";
        ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("12355.12345"));
        matchSingleRowExplain(query, "SELECT C7 FROM \"C##LOADER\".ALL_TYPES WHERE 12346 < C7");
    }

    @Test
    public void testAggregateSingleGroup() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT min(C7) FROM ALL_TYPES";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "12345.12345");
        matchSingleRowExplain(query, "SELECT CAST(MIN(C7) AS FLOAT) FROM \"C##LOADER\".ALL_TYPES");
    }

    @Test
    public void testAggregateGroupByColumn() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C5, min(C7) FROM ALL_TYPES GROUP BY C5";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "12345678901234567890", "12345.12345");
        matchNextRow(result, "1234567890.123456789", "12355.12345");
        matchSingleRowExplain(query, "SELECT TO_CHAR(C5), CAST(MIN(C7) AS FLOAT) FROM \"C##LOADER\".ALL_TYPES GROUP BY C5");
    }

    @Test
    public void testAggregateGroupByExpression() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C5 + 1, min(C7) FROM ALL_TYPES GROUP BY C5 + 1";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "12345678901234567891", "12345.12345");
        matchNextRow(result, "1234567891.123456789", "12355.12345");
        matchSingleRowExplain(query, "SELECT CAST((C5 + 1) AS FLOAT), CAST(MIN(C7) AS FLOAT) FROM \"C##LOADER\".ALL_TYPES GROUP BY (C5 + 1)");
    }

    @Test
    public void testAggregateGroupByTuple() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C3, C5, min(C7) FROM ALL_TYPES GROUP BY C3, C5 ORDER BY C5 DESC";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "cccccccccccccccccccc", "12345678901234567890", "12345.12345");
        matchNextRow(result, "cccccccccccccccccccc", "1234567890.123456789", "12355.12345");
        matchSingleRowExplain(query, "SELECT C3, TO_CHAR(C5), CAST(MIN(C7) AS FLOAT) FROM \"C##LOADER\".ALL_TYPES GROUP BY C3, C5 ORDER BY C5 DESC");
    }

    @Test
    public void testAggregateHaving() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C5, min(C7) FROM ALL_TYPES GROUP BY C5 HAVING MIN(C7) > 12350";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "1234567890.123456789", "12355.12345");
        matchSingleRowExplain(query, "SELECT TO_CHAR(C5), CAST(MIN(C7) AS FLOAT) FROM \"C##LOADER\".ALL_TYPES GROUP BY C5 HAVING 12350 < MIN(C7)");
    }

    @Test
    public void testOrderByColumn() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C1 FROM ALL_TYPES ORDER BY C1 DESC NULLS LAST";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "aaaaaaaaaaaaaaaaaaaa                              ");
        matchNextRow(result, (Object)null);
        matchSingleRowExplain(query, "SELECT C1 FROM \"C##LOADER\".ALL_TYPES ORDER BY C1 DESC NULLS LAST");
    }

    @Test
    public void testOrderByExpression() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C7 FROM ALL_TYPES ORDER BY ABS(C7) DESC NULLS FIRST";
        ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("12355.12345"));
        matchNextRow(result, new BigDecimal("12345.12345"));
        matchSingleRowExplain(query, "SELECT C7 FROM \"C##LOADER\".ALL_TYPES ORDER BY ABS(C7) DESC");
    }

    @Test
    public void testLimit() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C7 FROM ALL_TYPES ORDER BY C7 LIMIT 2";
        ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("12345.12345"));
        matchNextRow(result, new BigDecimal("12355.12345"));
        matchSingleRowExplain(query, "SELECT LIMIT_SUBSELECT.* FROM ( SELECT C7 FROM \"C##LOADER\".ALL_TYPES ORDER BY C7  ) LIMIT_SUBSELECT WHERE ROWNUM <= 2");
    }

    @Test
    public void testLimitOffset() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C7 FROM ALL_TYPES ORDER BY C7 LIMIT 1 OFFSET 1";
        ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("12355.12345"));
        matchSingleRowExplain(query, "SELECT c0 FROM ( SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( SELECT C7 AS c0 FROM \"C##LOADER\".ALL_TYPES ORDER BY C7  ) LIMIT_SUBSELECT WHERE ROWNUM <= 2 ) WHERE ROWNUM_SUB > 1");
    }

    private static void createOracleJDBCAdapter() throws SQLException, FileNotFoundException {
        String jdbcAdapterPath = getConfig().getJdbcAdapterPath();
        String oracleJdbcDriverdbcDriver = getConfig().getOracleJdbcDriverPath();
        List<String> includes = new ArrayList<>();
        includes.add(jdbcAdapterPath);
        includes.add(oracleJdbcDriverdbcDriver);
        createJDBCAdapter(includes);
    }

}
