package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.print.DocFlavor;
import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tested with Oracle 12
 *
 * TODO Add tests for data types
 * TODO Test Expanding of SELECT * if elements of select list require casting
 */
public class OracleSqlDialectIT extends AbstractIntegrationTest {

    private static final String VIRTUAL_SCHEMA_JDBC = "VS_ORACLE_JDBC";
    private static final String VIRTUAL_SCHEMA_IMPORT = "VS_ORACLE_IMPORT";
    private static final String ORACLE_SCHEMA = "LOADER";
    private static final String TEST_TABLE = "TYPE_TEST";

    private static final String EXA_TABLE_JDBC = VIRTUAL_SCHEMA_JDBC + "." + TEST_TABLE;
    private static final String EXA_TABLE_IMPORT = VIRTUAL_SCHEMA_IMPORT + "." + TEST_TABLE;
    private static final String ORA_TABLE = ORACLE_SCHEMA + "." + TEST_TABLE;

    private static final boolean IS_LOCAL = false;

    // Use getColumnTypes() to access this map
    private Map<String, String> columnTypes;

    @BeforeClass
    public static void beforeMethod() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().oracleTestsRequested());
        setConnection(connectToExa());

        createOracleJDBCAdapter();
        createOracleConnection();

        // create JDBC virtual schema
        createVirtualSchema(
                VIRTUAL_SCHEMA_JDBC,
                OracleSqlDialect.NAME,
                "",
                ORACLE_SCHEMA,
                "",
                getConfig().getOracleUser(),
                getConfig().getOraclePassword(),
                //"ADAPTER.JDBC_ORACLE_DEBUG",
                "ADAPTER.JDBC_ORACLE",
                getConfig().getOracleJdbcConnectionString(),
                IS_LOCAL,
                getConfig().debugAddress(),
                TEST_TABLE,null);

        // create IMPORT FROM ORA virtual schema
        createVirtualSchema(
                VIRTUAL_SCHEMA_IMPORT,
                OracleSqlDialect.NAME,
                "",
                ORACLE_SCHEMA,
                "",
                getConfig().getOracleUser(),
                getConfig().getOraclePassword(),
                //"ADAPTER.JDBC_ORACLE_DEBUG",
                "ADAPTER.JDBC_ORACLE",
                getConfig().getOracleJdbcConnectionString(),
                IS_LOCAL,
                getConfig().debugAddress(),
                TEST_TABLE,
                "IMPORT_FROM_ORA='true' ORA_CONNECTION_NAME='CONN_ORACLE'");
    }

    private static void createOracleJDBCAdapter() throws SQLException, FileNotFoundException {
        String jdbcAdapterPath = getConfig().getJdbcAdapterPath();
        String oracleJdbcDriverdbcDriver = getConfig().getOracleJdbcDriverPath();
        List<String> includes = new ArrayList<>();
        includes.add(jdbcAdapterPath);
        includes.add(oracleJdbcDriverdbcDriver);
        createJDBCAdapter(includes);
    }

    private String getColumnType(String column) throws SQLException {
        if (columnTypes == null) {
            columnTypes = getColumnTypesOfTable(EXA_TABLE_JDBC);
        }
        return columnTypes.get(column.toUpperCase());
    }

    private Map<String, String> getColumnTypesOfTable(String table) throws SQLException {
        Map<String, String> map = new HashMap<>();
        ResultSet result = executeQuery("DESCRIBE " + table);
        while (result.next()) {
            map.put(result.getString("COLUMN_NAME").toUpperCase(), result.getString("SQL_TYPE").toUpperCase());
        }
        return map;
    }

    private static void createOracleConnection() throws SQLException, FileNotFoundException {
        URI conn = getConfig().getOracleConnectionInformation();
        String connectionString = String.format("(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST = %s)(PORT = %d)))(CONNECT_DATA = (SERVICE_NAME = %s)))",
                conn.getHost(), conn.getPort(), conn.getPath().substring(1));
        createConnection("CONN_ORACLE", connectionString, getConfig().getOracleUser(), getConfig().getOraclePassword());
    }

    private List<ResultSet> runQuery(String query) throws SQLException {
        ArrayList<ResultSet> result = new ArrayList<>();
        result.add(executeQuery(String.format(query, EXA_TABLE_JDBC)));
        result.add(executeQuery(String.format(query, EXA_TABLE_IMPORT)));
        return result;
    }

    private void runMatchSingleRowExplain(String query, String expectedExplain) throws SQLException {
        matchSingleRowExplain(String.format(query, EXA_TABLE_JDBC), expectedExplain);
        matchSingleRowExplain(String.format(query, EXA_TABLE_IMPORT), expectedExplain);
    }

    private void matchNextRowDecimal(ResultSet result, String... expectedStrings) throws SQLException {
        result.next();
        if (result.getMetaData().getColumnCount() != expectedStrings.length) {
            throw new IllegalArgumentException(String.format("Row has %d columns but only %d arguments were given",
                    result.getMetaData().getColumnCount(), expectedStrings.length));
        }

        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            if (result.getObject(i) == null) continue;
            BigDecimal expected = new BigDecimal(expectedStrings[i-1]);
            BigDecimal actual = result.getBigDecimal(i).stripTrailingZeros();
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testColumnTypeEquivalence() throws SQLException {
        Map<String, String> jdbcColumnTypes = getColumnTypesOfTable(EXA_TABLE_JDBC);
        Map<String, String> importColumnTypes = getColumnTypesOfTable(EXA_TABLE_IMPORT);

        for (Map.Entry entry : jdbcColumnTypes.entrySet()) {
            assertEquals(entry.getValue(), importColumnTypes.get(entry.getKey()));
        }
    }

    @Test
    public void testSelectExpression() throws SQLException {
        String query = "SELECT C7 + 1 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "12346.12345");
            matchNextRowDecimal(result, "12356.12345");
        }
        runMatchSingleRowExplain(query, "SELECT CAST((C7 + 1) AS FLOAT) FROM " + ORA_TABLE);
    }

    @Test
    public void testFilterExpression() throws SQLException {
        String query = "SELECT C7 FROM %s WHERE C7 > 12346";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "12355.12345");
        }
        matchSingleRowExplain(query, "SELECT C7 FROM " + ORA_TABLE + " WHERE 12346 < C7");
    }

    @Test
    public void testAggregateSingleGroup() throws SQLException {
        String query = "SELECT min(C7) FROM %s";
        for(ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result,"12345.12345");
        }
        runMatchSingleRowExplain(query, "SELECT CAST(MIN(C7) AS FLOAT) FROM " + ORA_TABLE);
    }

    @Test
    public void testAggregateGroupByColumn() throws SQLException {
        String query = "SELECT C5, min(C7) FROM %s GROUP BY C5";
        for(ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "123456789012345678901234567890123456", "12345.12345");
            matchNextRowDecimal(result, "1234567890.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(C5), CAST(MIN(C7) AS FLOAT) FROM " + ORA_TABLE + " GROUP BY C5");
    }


    @Test
    public void testAggregateGroupByExpression() throws SQLException {
        String query = "SELECT C5 + 1, min(C7) FROM %s GROUP BY C5 + 1";
        for (ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "123456789012345678901234567890123457", "12345.12345");
            matchNextRowDecimal(result, "1234567891.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query, "SELECT CAST((C5 + 1) AS FLOAT), CAST(MIN(C7) AS FLOAT) FROM " + ORA_TABLE + " GROUP BY (C5 + 1)");
    }

    @Test
    public void testAggregateGroupByTuple() throws SQLException {
        String query = "SELECT C_NUMBER36, C5, min(C7) FROM %s GROUP BY C_NUMBER36, C5 ORDER BY C5 DESC";
        for (ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "123456789012345678901234567890123456", "123456789012345678901234567890123456", "12345.12345");
            matchNextRowDecimal(result, "123456789012345678901234567890123456", "1234567890.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query, "SELECT C_NUMBER36, TO_CHAR(C5), CAST(MIN(C7) AS FLOAT) FROM " + ORA_TABLE + " GROUP BY C5, C_NUMBER36 ORDER BY C5 DESC");
    }

    @Test
    public void testAggregateHaving() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT C5, min(C7) FROM %s GROUP BY C5 HAVING MIN(C7) > 12350";
        for (ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "1234567890.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(C5), CAST(MIN(C7) AS FLOAT) FROM " + ORA_TABLE + " GROUP BY C5 HAVING 12350 < MIN(C7)");
    }

    @Test
    public void testOrderByColumn() throws SQLException {
        String query = "SELECT C1 FROM %s ORDER BY C1 DESC NULLS LAST";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "aaaaaaaaaaaaaaaaaaaa                              ");
            matchNextRow(result, (Object) null);
        }
        runMatchSingleRowExplain(query, "SELECT C1 FROM " + ORA_TABLE + " ORDER BY C1 DESC NULLS LAST");
    }

    @Test
    public void testOrderByExpression() throws SQLException {
        String query = "SELECT C7 FROM %s ORDER BY ABS(C7) DESC NULLS FIRST";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "12355.12345");
            matchNextRow(result, "12345.12345");
        }
        matchSingleRowExplain(query, "SELECT C7 FROM " + ORA_TABLE + " ORDER BY ABS(C7) DESC");
    }

    @Test
    public void testLimit() throws SQLException {
        String query = "SELECT C7 FROM %s ORDER BY C7 LIMIT 2";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "12345.12345");
            matchNextRow(result, "12355.12345");
        }
        matchSingleRowExplain(query, "SELECT LIMIT_SUBSELECT.* FROM ( SELECT C7 FROM " + ORA_TABLE + " ORDER BY C7  ) LIMIT_SUBSELECT WHERE ROWNUM <= 2");
    }

    @Test
    public void testLimitOffset() throws SQLException {
        String query = "SELECT C7 FROM %s ORDER BY C7 LIMIT 1 OFFSET 1";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "12355.12345");
        }
        matchSingleRowExplain(query, "SELECT c0 FROM ( SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( SELECT C7 AS c0 FROM " + ORA_TABLE + " ORDER BY C7  ) LIMIT_SUBSELECT WHERE ROWNUM <= 2 ) WHERE ROWNUM_SUB > 1");
    }

    @Test
    public void testChar() throws SQLException {
        String query = "SELECT C1 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "aaaaaaaaaaaaaaaaaaaa                              ");
        }
        assertEquals("CHAR(50) ASCII", getColumnType("C1"));
    }

    @Test
    public void testNChar() throws SQLException {
        String query = "SELECT C2 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "bbbbbbbbbbbbbbbbbbbb                              ");
        }
        assertEquals("CHAR(50) UTF8", getColumnType("C2"));
    }

    @Test
    public void testVarchar() throws SQLException {
        String query = "SELECT C3 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "cccccccccccccccccccc");
        }
        assertEquals("VARCHAR(50) ASCII", getColumnType("C3"));
    }

    @Test
    public void testNVarchar() throws SQLException {
        String query = "SELECT C4 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "dddddddddddddddddddd");
        }
        assertEquals("VARCHAR(50) UTF8", getColumnType("C4"));
    }

    @Test
    public void testNumber() throws SQLException {
        String query = "SELECT C5 FROM %s";
        for (ResultSet result : runQuery(query)) {
            result.next();
            BigDecimal expected = new BigDecimal("123456789012345678901234567890123456");
            BigDecimal actual = result.getBigDecimal("C5");
            assertEquals(expected, actual);
        }
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C5"));
    }

    @Test
    public void testNumber36() throws SQLException {
        String query = "SELECT c_number36 FROM %s";
        for (ResultSet result : runQuery(query)) {
            result.next();
            BigDecimal expected = new BigDecimal("123456789012345678901234567890123456");
            BigDecimal actual = result.getBigDecimal("c_number36");
            assertEquals(expected, actual);
        }
        assertEquals("DECIMAL(36,0)", getColumnType("C_NUMBER36"));
    }

    @Test
    public void testNumber38() throws SQLException {
        String query = "SELECT C6 FROM %s";
        for (ResultSet result : runQuery(query)) {
            result.next();
            BigDecimal expected = new BigDecimal("12345678901234567890123456789012345678");
            BigDecimal actual = result.getBigDecimal("C6");
            assertEquals(expected, actual);
        }
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C6"));
    }

    @Test
    public void testNumber10S5() throws SQLException {
        String query = "SELECT C7 FROM %s";
        for (ResultSet result : runQuery(query)) {
            result.next();
            BigDecimal expected = new BigDecimal("12345.12345");
            BigDecimal actual = result.getBigDecimal("C7").stripTrailingZeros();
            assertEquals(expected, actual);
        }
        assertEquals("DECIMAL(10,5)", getColumnType("C7"));
    }

    @Test
    public void testBinaryFloat() throws SQLException {
        String query = "SELECT C_BINFLOAT FROM %s";
        for (ResultSet result : runQuery(query)) {
            result.next();
            Float expected = Float.parseFloat("1234.1241723");
            Float actual = result.getFloat(1);
            if (Math.abs(expected - actual) > 0.00001) {
                fail();
            }
        }
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C_BINFLOAT"));
    }

    @Test
    public void testBinaryDouble() throws SQLException {
        String query = "SELECT C_BINDOUBLE FROM %s";
        for (ResultSet result : runQuery(query)) {
            result.next();
            Double expected = Double.parseDouble("1234987.120871234");
            Double actual = result.getDouble(1);
            if (Math.abs(expected - actual) > 0.0000001) {
                fail();
            }
        }
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C_BINDOUBLE"));
    }

    @Test
    public void testFloat() throws SQLException {
        String query = "SELECT C_FLOAT FROM %s";
        for (ResultSet result : runQuery(query)) {
            result.next();
            Float expected = Float.parseFloat("12345.01982348239");
            Float actual = result.getFloat(1);
            if (Math.abs(expected - actual) > 0.000000001) {
                fail();
            }
        }
        assertEquals("DOUBLE", getColumnType("C_FLOAT"));
    }

    @Test
    public void testFloat126() throws SQLException {
        String query = "SELECT C_FLOAT126 FROM %s";
        for (ResultSet result : runQuery(query)) {
            result.next();
            Float expected = Float.parseFloat("12345678.01234567901234567890123456789");
            Float actual = result.getFloat(1);
            if (Math.abs(expected - actual) > 0.00000000000000000001) {
                fail();
            }
        }
        assertEquals("DOUBLE", getColumnType("C_FLOAT126"));
    }

    @Test
    public void testLong() throws SQLException {
        String query = "SELECT C_LONG FROM " + EXA_TABLE_JDBC;
        ResultSet result = executeQuery(query);
        matchNextRow(result, "test long 123");
        assertEquals("VARCHAR(2000000) ASCII", getColumnType("C_LONG"));
    }

    @Test
    public void testDate() throws SQLException {
        String query = "SELECT C10 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, Date.valueOf("2016-08-19"));
        }
        runMatchSingleRowExplain(query, "SELECT C10 FROM " + ORA_TABLE);
        assertEquals("TIMESTAMP", getColumnType("C10"));
    }

    @Test
    public void testTimestamp3() throws SQLException {
        String query = "SELECT C11 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "11-MAR-13 05.30.15.123 PM");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(C11) FROM " + ORA_TABLE);
        assertEquals("TIMESTAMP", getColumnType("C11"));
    }

    @Test
    public void testTimestamp6() throws SQLException {
        String query = "SELECT C12 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "11-MAR-13 05.30.15.123456 PM");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(C12) FROM " + ORA_TABLE);
        assertEquals("TIMESTAMP", getColumnType("C12"));
    }

    @Test
    public void testTimestamp9() throws SQLException {
        String query = "SELECT C13 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "11-MAR-13 05.30.15.123456789 PM");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(C13) FROM " + ORA_TABLE);
        assertEquals("TIMESTAMP", getColumnType("C13"));
    }

    @Test
    public void testTimestampTZ() throws SQLException {
        String query = "SELECT C14 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "19-AUG-16 11.28.05.000000 AM -08:00");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(C14) FROM " + ORA_TABLE);
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C14"));
    }

    @Test
    public void testTimestampLocalTZ() throws SQLException {
        executeUpdate("ALTER SESSION SET TIME_ZONE = 'UTC'");
        String query = "SELECT C15 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "30-APR-18 06.00.05.000000 PM");
        }
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C15"));
    }

    @Test
    public void testIntervalYear() throws SQLException {
        String query = "SELECT C16 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "+54-02");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(C16) FROM " + ORA_TABLE);
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C16"));
    }

    @Test
    public void testIntervalDay() throws SQLException {
        String query = "SELECT C17 FROM %s";
        for (ResultSet result : runQuery(query)) {
            matchNextRow(result, "+01 11:12:10.123000");
            matchNextRow(result, "+02 02:03:04.123456");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(C17) FROM " + ORA_TABLE);
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C17"));
    }
}
