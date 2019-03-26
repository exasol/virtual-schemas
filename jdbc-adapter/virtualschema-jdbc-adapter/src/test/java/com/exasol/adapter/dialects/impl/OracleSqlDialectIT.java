package com.exasol.adapter.dialects.impl;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.exasol.adapter.dialects.AbstractIntegrationTest;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

/**
 * Tested with Oracle 12
 *
 * TODO Add tests for data types TODO Test Expanding of SELECT * if elements of
 * select list require casting
 */
public class OracleSqlDialectIT extends AbstractIntegrationTest {

    private static final String VIRTUAL_SCHEMA_JDBC = "VS_ORACLE_JDBC";
    private static final String VIRTUAL_SCHEMA_ORA = "VS_ORACLE_IMPORT";
    private static final String ORACLE_SCHEMA = "SYSTEM";
    private static final String TEST_TABLE = "TYPE_TEST";

    private static final String EXA_TABLE_JDBC = VIRTUAL_SCHEMA_JDBC + "." + TEST_TABLE;
    private static final String EXA_TABLE_ORA = VIRTUAL_SCHEMA_ORA + "." + TEST_TABLE;
    private static final String ORA_TABLE = ORACLE_SCHEMA + "\".\"" + TEST_TABLE;

    private static final boolean IS_LOCAL = false;

    // Use getColumnTypes() to access this map
    private Map<String, String> columnTypesJDBC;
    private Map<String, String> columnTypesORA;

    @BeforeClass
    public static void beforeMethod() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().oracleTestsRequested());
        setConnection(connectToExa());

        createOracleJDBCAdapter();
        createOracleConnection();

        // create JDBC virtual schema
        createVirtualSchema(VIRTUAL_SCHEMA_JDBC, OracleSqlDialect.getPublicName(), "", ORACLE_SCHEMA, "",
                getConfig().getOracleUser(), getConfig().getOraclePassword(),
                // "ADAPTER.JDBC_ORACLE_DEBUG",
                "ADAPTER.JDBC_ADAPTER", getConfig().getOracleDockerJdbcConnectionString(), IS_LOCAL,
                getConfig().debugAddress(), "", null,"");

        // create IMPORT FROM ORA virtual schema
        createVirtualSchema(VIRTUAL_SCHEMA_ORA, OracleSqlDialect.getPublicName(), "", ORACLE_SCHEMA, "",
                getConfig().getOracleUser(), getConfig().getOraclePassword(),
                // "ADAPTER.JDBC_ORACLE_DEBUG",
                "ADAPTER.JDBC_ADAPTER", getConfig().getOracleDockerJdbcConnectionString(), IS_LOCAL,
                getConfig().debugAddress(), "", "IMPORT_FROM_ORA='true' ORA_CONNECTION_NAME='CONN_ORACLE'","");
    }

    private static void createOracleJDBCAdapter() throws SQLException, FileNotFoundException {
        final String jdbcAdapterPath = getConfig().getJdbcAdapterPath();
        final String oracleJdbcDriverdbcDriver = getConfig().getOracleJdbcDriverPath();
        final List<String> includes = new ArrayList<>();
        includes.add(jdbcAdapterPath);
        includes.add(oracleJdbcDriverdbcDriver);
        createJDBCAdapter(includes);
    }

    private String getColumnType(final String column) throws SQLException {
        return getColumnTypeJDBC(column);
    }

    private String getColumnTypeJDBC(final String column) throws SQLException {
        if (this.columnTypesJDBC == null) {
            this.columnTypesJDBC = getColumnTypesOfTable(EXA_TABLE_JDBC);
        }
        return this.columnTypesJDBC.get(column.toUpperCase());
    }

    private String getColumnTypeORA(final String column) throws SQLException {
        if (this.columnTypesORA == null) {
            this.columnTypesORA = getColumnTypesOfTable(EXA_TABLE_ORA);
        }
        return this.columnTypesORA.get(column.toUpperCase());
    }


    private Map<String, String> getColumnTypesOfTable(final String table) throws SQLException {
        final Map<String, String> map = new HashMap<>();
        final ResultSet result = executeQuery("DESCRIBE " + table);
        while (result.next()) {
            map.put(result.getString("COLUMN_NAME").toUpperCase(), result.getString("SQL_TYPE").toUpperCase());
        }
        return map;
    }

    private static void createOracleConnection() throws SQLException, FileNotFoundException {
        final URI conn = getConfig().getOracleDockerConnectionInformation();
        final String connectionString = String.format(
                "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST = %s)(PORT = %d)))(CONNECT_DATA = (SERVICE_NAME = %s)))",
                conn.getHost(), conn.getPort(), conn.getPath().substring(1));
        createConnection("CONN_ORACLE", connectionString, getConfig().getOracleUser(), getConfig().getOraclePassword());
    }

    private List<ResultSet> runQuery(final String query) throws SQLException {
        final ArrayList<ResultSet> result = new ArrayList<>();
        result.add(runQueryJDBC(query));
        result.add(runQueryORA(query));
        return result;
    }

    private ResultSet runQueryJDBC(final String query) throws SQLException {
        return executeQuery(String.format(query, EXA_TABLE_JDBC));
    }

    private ResultSet runQueryORA(final String query) throws SQLException {
        return executeQuery(String.format(query, EXA_TABLE_ORA));
    }

    private void runMatchSingleRowExplain(final String query, final String expectedExplain) throws SQLException {
        runMatchSingleRowExplainJDBC(query, expectedExplain);
        runMatchSingleRowExplainORA(query, expectedExplain);
    }

    private void runMatchSingleRowExplainJDBC(final String query, final String expectedExplain) throws SQLException {
        matchSingleRowExplain(String.format(query, EXA_TABLE_JDBC), expectedExplain);
    }

    private void runMatchSingleRowExplainORA(final String query, final String expectedExplain) throws SQLException {
        matchSingleRowExplain(String.format(query, EXA_TABLE_ORA), expectedExplain);
    }

    private void matchNextRowDecimal(final ResultSet result, final String... expectedStrings) throws SQLException {
        result.next();
        if (result.getMetaData().getColumnCount() != expectedStrings.length) {
            throw new IllegalArgumentException(String.format("Row has %d columns but only %d arguments were given",
                    result.getMetaData().getColumnCount(), expectedStrings.length));
        }

        for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            if (result.getObject(i) == null) {
                continue;
            }
            final BigDecimal expected = new BigDecimal(expectedStrings[i - 1]);
            final BigDecimal actual = result.getBigDecimal(i).stripTrailingZeros();
            assertEquals(expected, actual);
        }
    }

    // Join Tests -------------------------------------------------------------
    @Test
    public void innerJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a INNER JOIN  %1$s.t2 b ON a.x=b.x", VIRTUAL_SCHEMA_ORA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "2", "bbb", "2" ,"bbb");
        assertFalse(result.next());
    }

    @Test
    public void innerJoinWithProjection() throws SQLException {
        final String query = String.format("SELECT b.y || %1$s.t1.y FROM  %1$s.t1 INNER JOIN  %1$s.t2 b ON %1$s.t1.x=b.x", VIRTUAL_SCHEMA_JDBC);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "bbbbbb");
        assertFalse(result.next());
    }

    @Test
    public void leftJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a LEFT OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA_ORA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "1", "aaa", null ,null);
        matchNextRow(result, "2", "bbb", "2" ,"bbb");
        assertFalse(result.next());
    }

    @Test
    public void rightJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a RIGHT OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA_JDBC);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "2", "bbb", "2" ,"bbb");
        matchNextRow(result, null, null, "3" ,"ccc");
        assertFalse(result.next());
    }

    @Test
    public void fullOuterJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a FULL OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA_ORA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "1", "aaa", null ,null);
        matchNextRow(result, "2", "bbb", "2" ,"bbb");
        matchNextRow(result, null, null, "3" ,"ccc");
        assertFalse(result.next());
    }

    // Type Tests -------------------------------------------------------------
    @Test
    public void testColumnTypeEquivalence() throws SQLException {
        final Map<String, String> jdbcColumnTypes = getColumnTypesOfTable(EXA_TABLE_JDBC);
        final Map<String, String> importColumnTypes = getColumnTypesOfTable(EXA_TABLE_ORA);

        for (final Map.Entry entry : jdbcColumnTypes.entrySet()) {
            assertEquals(entry.getValue(), importColumnTypes.get(entry.getKey()));
        }
    }

    @Test
    public void testSelectExpression() throws SQLException {
        final String query = "SELECT C7 + 1 FROM %s ORDER BY 1";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "12346.12345");
            matchNextRowDecimal(result, "12356.12345");
        }
        runMatchSingleRowExplain(query, "SELECT CAST((\"C7\" + 1) AS FLOAT) FROM \"" + ORA_TABLE + "\" ORDER BY (\"C7\" + 1)");
    }

    @Test
    public void testFilterExpression() throws SQLException {
        final String query = "SELECT C7 FROM %s WHERE C7 > 12346";
        ResultSet resultJDBC = runQueryJDBC(query);
        matchNextRow(resultJDBC, "12355.12345");
        ResultSet resultORA = runQueryORA(query);
        matchNextRow(resultORA, "01.2355123450E4");

        runMatchSingleRowExplain(query, "SELECT \"C7\" FROM \"" + ORA_TABLE + "\" WHERE 12346 < \"C7\"");
    }

    @Test
    public void testAggregateSingleGroup() throws SQLException {
        final String query = "SELECT min(C7) FROM %s";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "12345.12345");
        }
        runMatchSingleRowExplain(query, "SELECT CAST(MIN(\"C7\") AS FLOAT) FROM \"" + ORA_TABLE + "\"");
    }

    @Test
    public void testAggregateGroupByColumn() throws SQLException {
        final String query = "SELECT C5, min(C7) FROM %s GROUP BY C5 ORDER BY 1 DESC";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "123456789012345678901234567890123456", "12345.12345");
            matchNextRowDecimal(result, "1234567890.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query,
                "SELECT TO_CHAR(\"C5\"), CAST(MIN(\"C7\") AS FLOAT) FROM \"" + ORA_TABLE + "\" GROUP BY \"C5\" ORDER BY \"C5\" DESC");
    }

    @Test
    public void testAggregateGroupByExpression() throws SQLException {
        final String query = "SELECT C5 + 1, min(C7) FROM %s GROUP BY C5 + 1 ORDER BY 1 DESC";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "123456789012345678901234567890123457", "12345.12345");
            matchNextRowDecimal(result, "1234567891.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query,
                "SELECT CAST((\"C5\" + 1) AS FLOAT), CAST(MIN(\"C7\") AS FLOAT) FROM \"" + ORA_TABLE + "\" GROUP BY (\"C5\" + 1) ORDER BY (\"C5\" + 1) DESC");
    }

    @Test
    public void testAggregateGroupByTuple() throws SQLException {
        final String query = "SELECT C_NUMBER36, C5, min(C7) FROM %s GROUP BY C_NUMBER36, C5 ORDER BY C5 DESC";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "123456789012345678901234567890123456", "123456789012345678901234567890123456",
                    "12345.12345");
            matchNextRowDecimal(result, "123456789012345678901234567890123456", "1234567890.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query, "SELECT \"C_NUMBER36\", TO_CHAR(\"C5\"), CAST(MIN(\"C7\") AS FLOAT) FROM \"" + ORA_TABLE
                + "\" GROUP BY \"C5\", \"C_NUMBER36\" ORDER BY \"C5\" DESC");
    }

    @Test
    public void testAggregateHaving() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String query = "SELECT C5, min(C7) FROM %s GROUP BY C5 HAVING MIN(C7) > 12350";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "1234567890.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query,
                "SELECT TO_CHAR(\"C5\"), CAST(MIN(\"C7\") AS FLOAT) FROM \"" + ORA_TABLE + "\" GROUP BY \"C5\" HAVING 12350 < MIN(\"C7\")");
    }

    @Test
    public void testOrderByColumn() throws SQLException {
        final String query = "SELECT C1 FROM %s ORDER BY C1 DESC NULLS LAST";
        for (final ResultSet result : runQuery(query)) {
            matchNextRow(result, "aaaaaaaaaaaaaaaaaaaa                              ");
            matchNextRow(result, (Object) null);
        }
        runMatchSingleRowExplain(query, "SELECT \"C1\" FROM \"" + ORA_TABLE + "\" ORDER BY \"C1\" DESC NULLS LAST");
    }

    @Test
    public void testOrderByExpression() throws SQLException {
        final String query = "SELECT C7 FROM %s ORDER BY ABS(C7) DESC NULLS FIRST";
        ResultSet resultJDBC = runQueryJDBC(query);
        matchNextRow(resultJDBC, "12355.12345");
        matchNextRow(resultJDBC, "12345.12345");
        ResultSet resultORA = runQueryORA(query);
        matchNextRow(resultORA, "01.2355123450E4");
        matchNextRow(resultORA, "01.2345123450E4");

        runMatchSingleRowExplain(query, "SELECT \"C7\" FROM \"" + ORA_TABLE + "\" ORDER BY ABS(\"C7\") DESC");
    }

    @Test
    public void testLimit() throws SQLException {
        final String query = "SELECT C7 FROM %s ORDER BY C7 LIMIT 2";
        ResultSet resultJDBC = runQueryJDBC(query);
        matchNextRow(resultJDBC, "12345.12345");
        matchNextRow(resultJDBC, "12355.12345");
        ResultSet resultORA = runQueryORA(query);
        matchNextRow(resultORA, "01.2345123450E4");
        matchNextRow(resultORA, "01.2355123450E4");

        runMatchSingleRowExplain(query, "SELECT LIMIT_SUBSELECT.* FROM ( SELECT \"C7\" FROM \"" + ORA_TABLE
                + "\" ORDER BY \"C7\"  ) LIMIT_SUBSELECT WHERE ROWNUM <= 2");
    }

    @Test
    public void testLimitOffset() throws SQLException {
        final String query = "SELECT C7 FROM %s ORDER BY C7 LIMIT 1 OFFSET 1";
        ResultSet resultJDBC = runQueryJDBC(query);
        matchNextRow(resultJDBC, "12355.12345");
        ResultSet resultORA = runQueryORA(query);
        matchNextRow(resultORA, "01.2355123450E4");

        runMatchSingleRowExplain(query,
                "SELECT c0 FROM ( SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( SELECT \"C7\" AS c0 FROM \"" + ORA_TABLE
                        + "\" ORDER BY \"C7\"  ) LIMIT_SUBSELECT WHERE ROWNUM <= 2 ) WHERE ROWNUM_SUB > 1");
    }

    @Test
    public void testChar() throws SQLException {
        final String query = "SELECT C1 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            matchNextRow(result, "aaaaaaaaaaaaaaaaaaaa                              ");
        }
        assertEquals("CHAR(50) ASCII", getColumnType("C1"));
    }

    @Test
    public void testNChar() throws SQLException {
        final String query = "SELECT C2 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            matchNextRow(result, "bbbbbbbbbbbbbbbbbbbb                              ");
        }
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C2"));
    }

    @Test
    public void testVarchar() throws SQLException {
        final String query = "SELECT C3 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            matchNextRow(result, "cccccccccccccccccccc");
        }
        assertEquals("VARCHAR(50) ASCII", getColumnType("C3"));
    }

    @Test
    public void testNVarchar() throws SQLException {
        final String query = "SELECT C4 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            matchNextRow(result, "dddddddddddddddddddd");
        }
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C4"));
    }

    @Test
    public void testNumber() throws SQLException {
        final String query = "SELECT C5 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            result.next();
            final BigDecimal expected = new BigDecimal("123456789012345678901234567890123456");
            final BigDecimal actual = result.getBigDecimal("C5");
            assertEquals(expected, actual);
        }
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C5"));
    }

    @Test
    public void testNumber36() throws SQLException {
        final String query = "SELECT c_number36 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            result.next();
            final BigDecimal expected = new BigDecimal("123456789012345678901234567890123456");
            final BigDecimal actual = result.getBigDecimal("c_number36");
            assertEquals(expected, actual);
        }
        assertEquals("DECIMAL(36,0)", getColumnType("C_NUMBER36"));
    }

    @Test
    public void testNumber38() throws SQLException {
        final String query = "SELECT C6 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            result.next();
            final BigDecimal expected = new BigDecimal("12345678901234567890123456789012345678");
            final BigDecimal actual = result.getBigDecimal("C6");
            assertEquals(expected, actual);
        }
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C6"));
    }

    @Test
    public void testNumber10S5() throws SQLException {
        final String query = "SELECT C7 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            result.next();
            final BigDecimal expected = new BigDecimal("12345.12345");
            final BigDecimal actual = result.getBigDecimal("C7").stripTrailingZeros();
            assertEquals(expected, actual);
        }
        assertEquals("DECIMAL(10,5)", getColumnType("C7"));
    }

    @Test
    public void testBinaryFloat() throws SQLException {
        final String query = "SELECT C_BINFLOAT FROM %s";
        for (final ResultSet result : runQuery(query)) {
            result.next();
            final Float expected = Float.parseFloat("1234.1241723");
            final Float actual = result.getFloat(1);
            if (Math.abs(expected - actual) > 0.00001) {
                fail();
            }
        }
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C_BINFLOAT"));
    }

    @Test
    public void testBinaryDouble() throws SQLException {
        final String query = "SELECT C_BINDOUBLE FROM %s";
        for (final ResultSet result : runQuery(query)) {
            result.next();
            final Double expected = Double.parseDouble("1234987.120871234");
            final Double actual = result.getDouble(1);
            if (Math.abs(expected - actual) > 0.0000001) {
                fail();
            }
        }
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C_BINDOUBLE"));
    }

    @Test
    public void testFloat() throws SQLException {
        final String query = "SELECT C_FLOAT FROM %s";
        for (final ResultSet result : runQuery(query)) {
            result.next();
            final Float expected = Float.parseFloat("12345.01982348239");
            final Float actual = result.getFloat(1);
            if (Math.abs(expected - actual) > 0.000000001) {
                fail();
            }
        }
        assertEquals("DOUBLE", getColumnType("C_FLOAT"));
    }

    @Test
    public void testFloat126() throws SQLException {
        final String query = "SELECT C_FLOAT126 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            result.next();
            final Float expected = Float.parseFloat("12345678.01234567901234567890123456789");
            final Float actual = result.getFloat(1);
            if (Math.abs(expected - actual) > 0.00000000000000000001) {
                fail();
            }
        }
        assertEquals("DOUBLE", getColumnType("C_FLOAT126"));
    }

    @Test
    public void testLong() throws SQLException {
        final String query = "SELECT C_LONG FROM " + EXA_TABLE_JDBC;
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "test long 123");
        assertEquals("VARCHAR(2000000) ASCII", getColumnType("C_LONG"));
    }

    @Test
    public void testDate() throws SQLException {
        final String query = "SELECT C10 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            matchNextRow(result, Date.valueOf("2016-08-19"));
        }
        runMatchSingleRowExplain(query, "SELECT \"C10\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("TIMESTAMP", getColumnType("C10"));
    }

    @Test
    public void testTimestamp3() throws SQLException {
        final String query = "SELECT C11 FROM %s";
        ResultSet resultJDBC = runQueryJDBC(query);
        matchNextRow(resultJDBC, "11-MAR-13 05.30.15.123 PM");
        ResultSet resultORA = runQueryORA(query);
        matchNextRow(resultORA,Timestamp.valueOf("2013-03-11 17:30:15.123"));

        runMatchSingleRowExplainJDBC(query, "SELECT TO_CHAR(\"C11\") FROM \"" + ORA_TABLE + "\"");
        runMatchSingleRowExplainORA(query, "SELECT \"C11\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("VARCHAR(2000000) UTF8", getColumnTypeJDBC("C11"));
        assertEquals("VARCHAR(2000000) UTF8", getColumnTypeORA("C11"));
    }

    @Test
    public void testTimestamp6() throws SQLException {
        final String query = "SELECT C12 FROM %s";
        ResultSet resultJDBC = runQueryJDBC(query);
        matchNextRow(resultJDBC, "11-MAR-13 05.30.15.123456 PM");
        ResultSet resultORA = runQueryORA(query);
        matchNextRow(resultORA,Timestamp.valueOf("2013-03-11 17:30:15.123"));

        runMatchSingleRowExplainJDBC(query, "SELECT TO_CHAR(\"C12\") FROM \"" + ORA_TABLE + "\"");
        runMatchSingleRowExplainORA(query, "SELECT \"C12\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("TIMESTAMP", getColumnTypeJDBC("C12"));
        assertEquals("TIMESTAMP", getColumnTypeORA("C12"));
    }

    @Test
    public void testTimestamp9() throws SQLException {
        final String query = "SELECT C13 FROM %s";
        ResultSet resultJDBC = runQueryJDBC(query);
        matchNextRow(resultJDBC, "11-MAR-13 05.30.15.123456789 PM");
        ResultSet resultORA = runQueryORA(query);
        matchNextRow(resultORA,Timestamp.valueOf("2013-03-11 17:30:15.123"));

        runMatchSingleRowExplainJDBC(query, "SELECT TO_CHAR(\"C13\") FROM \"" + ORA_TABLE + "\"");
        runMatchSingleRowExplainORA(query, "SELECT \"C13\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("VARCHAR(2000000) UTF8", getColumnTypeJDBC("C13"));
        assertEquals("VARCHAR(2000000) UTF8", getColumnTypeORA("C13"));
    }

    @Test
    public void testTimestampTZ() throws SQLException {
        final String query = "SELECT C14 FROM %s";
        ResultSet resultJDBC = runQueryJDBC(query);
        matchNextRow(resultJDBC, "19-AUG-16 11.28.05.000000 AM -08:00");
        ResultSet resultORA = runQueryORA(query);
        matchNextRow(resultORA, Timestamp.valueOf("2016-08-19 19:28:05.000"));

        runMatchSingleRowExplainJDBC(query, "SELECT TO_CHAR(\"C14\") FROM \"" + ORA_TABLE + "\"");
        runMatchSingleRowExplainORA(query, "SELECT \"C14\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("VARCHAR(2000000) UTF8", getColumnTypeJDBC("C14"));
        assertEquals("VARCHAR(2000000) UTF8", getColumnTypeORA("C14"));
    }

    @Test
    public void testTimestampLocalTZ() throws SQLException {
        executeUpdate("ALTER SESSION SET TIME_ZONE = 'UTC'");
        final String query = "SELECT C15 FROM %s";
        ResultSet resultJDBC = runQueryJDBC(query);
        matchNextRow(resultJDBC, "30-APR-18 06.00.05.000000 PM");
        ResultSet resultORA = runQueryORA(query);
        matchNextRow(resultORA, Timestamp.valueOf("2018-04-30 18:00:05.000"));

        runMatchSingleRowExplainJDBC(query, "SELECT TO_CHAR(\"C15\") FROM \"" + ORA_TABLE + "\"");
        runMatchSingleRowExplainORA(query, "SELECT \"C15\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("VARCHAR(2000000) UTF8", getColumnTypeJDBC("C15"));
        assertEquals("VARCHAR(2000000) UTF8", getColumnTypeORA("C15"));
    }

    @Test
    public void testIntervalYear() throws SQLException {
        final String query = "SELECT C16 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            matchNextRow(result, "+54-02");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(\"C16\") FROM \"" + ORA_TABLE + "\"");
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C16"));
    }

    @Test
    public void testIntervalDay() throws SQLException {
        final String query = "SELECT C17 FROM %s ORDER BY 1";
        for (final ResultSet result : runQuery(query)) {
            matchNextRow(result, "+01 11:12:10.123000");
            matchNextRow(result, "+02 02:03:04.123456");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(\"C17\") FROM \"" + ORA_TABLE + "\" ORDER BY \"C17\"");
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C17"));
    }

    @Test
    public void testSelectAllTimestampColumns() throws SQLException {
        executeUpdate("ALTER SESSION SET TIME_ZONE = 'UTC'");
        final String query = "SELECT * FROM %s.%s";
        ResultSet resultJDBC = runQueryJDBC(String.format(query, VIRTUAL_SCHEMA_JDBC, "TS_T"));
        matchNextRow(resultJDBC, "01-JAN-18 11.00.00.000000 AM", "01-JAN-18 10.00.00.000000 AM", "01-JAN-18 11.00.00.000000 AM +01:00");
        ResultSet resultORA = runQueryORA(String.format(query, VIRTUAL_SCHEMA_ORA, "TS_T"));
        matchNextRow(resultORA,Timestamp.valueOf("2018-01-01 11:00:00.000"), Timestamp.valueOf("2018-01-01 10:00:00.000"), Timestamp.valueOf("2018-01-01 10:00:00.000"));
        Map<String, String> columnTypesJDBC = getColumnTypesOfTable(VIRTUAL_SCHEMA_JDBC + ".TS_T");
        Map<String, String> columnTypesORA = getColumnTypesOfTable(VIRTUAL_SCHEMA_ORA + ".TS_T");

        assertEquals("TIMESTAMP", columnTypesJDBC.get("A"));
        assertEquals("TIMESTAMP", columnTypesORA.get("A"));
        assertEquals("VARCHAR(2000000) UTF8",  columnTypesJDBC.get("B"));
        assertEquals("VARCHAR(2000000) UTF8", columnTypesORA.get("B"));
        assertEquals("VARCHAR(2000000) UTF8",  columnTypesJDBC.get("C"));
        assertEquals("VARCHAR(2000000) UTF8", columnTypesORA.get("C"));
    }
}
