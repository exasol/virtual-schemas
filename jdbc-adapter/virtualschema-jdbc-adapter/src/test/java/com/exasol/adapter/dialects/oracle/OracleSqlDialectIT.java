package com.exasol.adapter.dialects.oracle;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.*;
import java.util.*;

import org.junit.Assume;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import com.exasol.adapter.dialects.IntegrationTestConfigurationCondition;

@Tag("integration")
@ExtendWith(IntegrationTestConfigurationCondition.class)
class OracleSqlDialectIT extends AbstractIntegrationTest {
    private static final String VIRTUAL_SCHEMA_JDBC = "VS_ORACLE_JDBC";
    private static final String VIRTUAL_SCHEMA_ORA = "VS_ORACLE_IMPORT";
    private static final String VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL = "VS_ORACLE_JDBC_NUMBER_TO_DECIMAL";
    private static final String VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL = "VS_ORACLE_ORA_NUMBER_TO_DECIMAL";
    private static final String ORACLE_SCHEMA = "LOADER";
    private static final String TEST_TABLE = "TYPE_TEST";
    private static final String NUMBER_TABLE = "NUMBER_T";

    private static final String EXA_TABLE_JDBC = VIRTUAL_SCHEMA_JDBC + "." + TEST_TABLE;
    private static final String EXA_TABLE_ORA = VIRTUAL_SCHEMA_ORA + "." + TEST_TABLE;
    private static final String ORA_TABLE = ORACLE_SCHEMA + "\".\"" + TEST_TABLE;
    private static final String ORA_NUMBER_TABLE = ORACLE_SCHEMA + "\".\"" + NUMBER_TABLE;
    private static final String EXA_TABLE_JDBC_NUMBER_TO_DECIMAL = VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL + "."
            + TEST_TABLE;
    private static final String ORA_TABLE_ORA_NUMBER_TO_DECIMAL = VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL + "."
            + TEST_TABLE;
    private static final String EXA_NUMBER_TABLE_JDBC_NUMBER_TO_DECIMAL = VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL + "."
            + NUMBER_TABLE;
    private static final String ORA_NUMBER_TABLE_ORA_NUMBER_TO_DECIMAL = VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL + "."
            + NUMBER_TABLE;

    private static final boolean IS_LOCAL = false;
    private static final String NUMBER_T = "\"NUMBER_T\"";
    private static final String TYPE_TEST_T = "\"TYPE_TEST\"";

    // Use getColumnTypes() to access this map
    private Map<String, String> columnTypesJDBC;
    private Map<String, String> columnTypesORA;

    @BeforeAll
    static void beforeMethod() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().oracleTestsRequested());
        setConnection(connectToExa());
        createOracleJDBCAdapter();
        createOracleConnection();

        // create JDBC virtual schema
        createVirtualSchema(VIRTUAL_SCHEMA_JDBC, OracleSqlDialect.NAME, "", ORACLE_SCHEMA, "",
                getConfig().getOracleUser(), getConfig().getOraclePassword(),
                // "ADAPTER.JDBC_ORACLE_DEBUG",
                "ADAPTER.JDBC_ADAPTER", getConfig().getOracleDockerJdbcConnectionString(), IS_LOCAL,
                getConfig().debugAddress(), "", null, "");

        // create IMPORT FROM ORA virtual schema
        createVirtualSchema(VIRTUAL_SCHEMA_ORA, OracleSqlDialect.NAME, "", ORACLE_SCHEMA, "",
                getConfig().getOracleUser(), getConfig().getOraclePassword(),
                // "ADAPTER.JDBC_ORACLE_DEBUG",
                "ADAPTER.JDBC_ADAPTER", getConfig().getOracleDockerJdbcConnectionString(), IS_LOCAL,
                getConfig().debugAddress(), "", "IMPORT_FROM_ORA='true' ORA_CONNECTION_NAME='CONN_ORACLE'", "");

        // create JDBC virtual schema with special NUMBER handling
        createVirtualSchema(VIRTUAL_SCHEMA_JDBC_NUMBER_TO_DECIMAL, OracleSqlDialect.NAME, "", ORACLE_SCHEMA, "",
                getConfig().getOracleUser(), getConfig().getOraclePassword(),
                // "ADAPTER.JDBC_ORACLE_DEBUG",
                "ADAPTER.JDBC_ADAPTER", getConfig().getOracleDockerJdbcConnectionString(), IS_LOCAL,
                getConfig().debugAddress(), "", "oracle_cast_number_to_decimal_with_precision_and_scale='36,1'", "");

        // create IMPORT FROM ORA virtual schema with special NUMBER handling
        createVirtualSchema(VIRTUAL_SCHEMA_ORA_NUMBER_TO_DECIMAL, OracleSqlDialect.NAME, "", ORACLE_SCHEMA, "",
                getConfig().getOracleUser(), getConfig().getOraclePassword(),
                // "ADAPTER.JDBC_ORACLE_DEBUG",
                "ADAPTER.JDBC_ADAPTER", getConfig().getOracleDockerJdbcConnectionString(), IS_LOCAL,
                getConfig().debugAddress(), "",
                "IMPORT_FROM_ORA='true' ORA_CONNECTION_NAME='CONN_ORACLE' oracle_cast_number_to_decimal_with_precision_and_scale='36,1'",
                "");
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
        return runQuery(query, EXA_TABLE_JDBC, EXA_TABLE_ORA);
    }

    private List<ResultSet> runQuery(final String query, final String jdbcTable, final String oraTable)
            throws SQLException {
        final ArrayList<ResultSet> result = new ArrayList<>();
        result.add(runQueryJDBC(query, jdbcTable));
        result.add(runQueryORA(query, oraTable));
        return result;
    }

    private ResultSet runQueryJDBC(final String query) throws SQLException {
        return executeQuery(String.format(query, EXA_TABLE_JDBC));
    }

    private ResultSet runQueryORA(final String query) throws SQLException {
        return executeQuery(String.format(query, EXA_TABLE_ORA));
    }

    private ResultSet runQueryJDBC(final String query, final String table) throws SQLException {
        return executeQuery(String.format(query, table));
    }

    private ResultSet runQueryORA(final String query, final String table) throws SQLException {
        return executeQuery(String.format(query, table));
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

    // Number handling --------------------------------------------------------
    @Test
    void testNumberToDecimal() {
        final String query = "SELECT C5 FROM %s";
        assertThrows(SQLException.class,
                () -> runQuery(query, EXA_TABLE_JDBC_NUMBER_TO_DECIMAL, ORA_TABLE_ORA_NUMBER_TO_DECIMAL));
    }

    @Test
    void testNumber36ToDecimal() throws SQLException {
        final String query = "SELECT c_number36 FROM %s";
        for (final ResultSet result : runQuery(query, EXA_TABLE_JDBC_NUMBER_TO_DECIMAL,
                ORA_TABLE_ORA_NUMBER_TO_DECIMAL)) {
            result.next();
            final BigDecimal expected = new BigDecimal("123456789012345678901234567890123456");
            final BigDecimal actual = result.getBigDecimal("c_number36");
            assertEquals(expected, actual);
        }
        assertEquals("DECIMAL(36,0)", getColumnType("C_NUMBER36"));
    }

    @Test
    void testNumber38ToDecimal() {
        final String query = "SELECT C6 FROM %s";
        assertThrows(SQLException.class,
                () -> runQuery(query, EXA_TABLE_JDBC_NUMBER_TO_DECIMAL, ORA_TABLE_ORA_NUMBER_TO_DECIMAL));
    }

    @Test
    void testNumber10S5ToDecimal() throws SQLException {
        final String query = "SELECT C7 FROM %s";
        for (final ResultSet result : runQuery(query, EXA_TABLE_JDBC_NUMBER_TO_DECIMAL,
                ORA_TABLE_ORA_NUMBER_TO_DECIMAL)) {
            result.next();
            final BigDecimal expected = new BigDecimal("12345.12345");
            final BigDecimal actual = result.getBigDecimal("C7").stripTrailingZeros();
            assertEquals(expected, actual);
        }
        assertEquals("DECIMAL(10,5)", getColumnType("C7"));
    }

    @Test
    void testSelectAllColsNumber() throws SQLException {
        final String query = "SELECT * FROM %s";
        for (final ResultSet result : runQuery(query, EXA_NUMBER_TABLE_JDBC_NUMBER_TO_DECIMAL,
                ORA_NUMBER_TABLE_ORA_NUMBER_TO_DECIMAL)) {
            matchNextRowDecimal(result, "1234567890123456789012345678901234.6", "1234567890123456789012345678.9",
                    "1234567890123456789012345678901234.56");
        }

        matchSingleRowExplain(String.format(query, EXA_NUMBER_TABLE_JDBC_NUMBER_TO_DECIMAL),
                "SELECT CAST(\"A\" AS DECIMAL(36,1)), CAST(\"B\" AS DECIMAL(36,1)), \"C\" FROM \"" + ORA_NUMBER_TABLE
                        + "\"");
        matchSingleRowExplain(String.format(query, ORA_NUMBER_TABLE_ORA_NUMBER_TO_DECIMAL),
                "SELECT CAST(\"A\" AS DECIMAL(36,1)), CAST(\"B\" AS DECIMAL(36,1)), \"C\" FROM \"" + ORA_NUMBER_TABLE
                        + "\"");

        final Map<String, String> columnTypesJDBC = getColumnTypesOfTable(EXA_NUMBER_TABLE_JDBC_NUMBER_TO_DECIMAL);
        final Map<String, String> columnTypesORA = getColumnTypesOfTable(ORA_NUMBER_TABLE_ORA_NUMBER_TO_DECIMAL);

        assertEquals("DECIMAL(36,1)", columnTypesJDBC.get("A"));
        assertEquals("DECIMAL(36,1)", columnTypesORA.get("A"));
        assertEquals("DECIMAL(36,1)", columnTypesJDBC.get("B"));
        assertEquals("DECIMAL(36,1)", columnTypesORA.get("B"));
        assertEquals("DECIMAL(36,2)", columnTypesJDBC.get("C"));
        assertEquals("DECIMAL(36,2)", columnTypesORA.get("C"));
    }

    @Test
    void testSelectNumber() throws SQLException {
        final String query = "SELECT a FROM %s";
        for (final ResultSet result : runQuery(query, EXA_NUMBER_TABLE_JDBC_NUMBER_TO_DECIMAL,
                ORA_NUMBER_TABLE_ORA_NUMBER_TO_DECIMAL)) {
            matchNextRowDecimal(result, "1234567890123456789012345678901234.6");
        }
        matchSingleRowExplain(String.format(query, EXA_NUMBER_TABLE_JDBC_NUMBER_TO_DECIMAL),
                "SELECT CAST(" + NUMBER_T + ".\"A\" AS DECIMAL(36,1)) FROM \"" + ORA_NUMBER_TABLE + "\"");
        matchSingleRowExplain(String.format(query, ORA_NUMBER_TABLE_ORA_NUMBER_TO_DECIMAL),
                "SELECT CAST(" + NUMBER_T + ".\"A\" AS DECIMAL(36,1)) FROM \"" + ORA_NUMBER_TABLE + "\"");
    }

    @Test
    void testSelectNumber3810() throws SQLException {
        final String query = "SELECT b FROM %s";
        for (final ResultSet result : runQuery(query, EXA_NUMBER_TABLE_JDBC_NUMBER_TO_DECIMAL,
                ORA_NUMBER_TABLE_ORA_NUMBER_TO_DECIMAL)) {
            matchNextRowDecimal(result, "1234567890123456789012345678.9");
        }
        matchSingleRowExplain(String.format(query, EXA_NUMBER_TABLE_JDBC_NUMBER_TO_DECIMAL),
                "SELECT CAST(" + NUMBER_T + ".\"B\" AS DECIMAL(36,1)) FROM \"" + ORA_NUMBER_TABLE + "\"");
        matchSingleRowExplain(String.format(query, ORA_NUMBER_TABLE_ORA_NUMBER_TO_DECIMAL),
                "SELECT CAST(" + NUMBER_T + ".\"B\" AS DECIMAL(36,1)) FROM \"" + ORA_NUMBER_TABLE + "\"");
    }

    @Test
    void testSelectNumber3602() throws SQLException {
        final String query = "SELECT c FROM %s";
        for (final ResultSet result : runQuery(query, EXA_NUMBER_TABLE_JDBC_NUMBER_TO_DECIMAL,
                ORA_NUMBER_TABLE_ORA_NUMBER_TO_DECIMAL)) {
            matchNextRowDecimal(result, "1234567890123456789012345678901234.56");
        }

        matchSingleRowExplain(String.format(query, EXA_NUMBER_TABLE_JDBC_NUMBER_TO_DECIMAL),
                "SELECT " + NUMBER_T + ".\"C\" FROM \"" + ORA_NUMBER_TABLE + "\"");
        matchSingleRowExplain(String.format(query, ORA_NUMBER_TABLE_ORA_NUMBER_TO_DECIMAL),
                "SELECT " + NUMBER_T + ".\"C\" FROM \"" + ORA_NUMBER_TABLE + "\"");
    }

    // Join Tests -------------------------------------------------------------
    @Test
    void testInnerJoin() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA_ORA + ".t1 a INNER JOIN  " + VIRTUAL_SCHEMA_ORA
                + ".t2 b ON a.x=b.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> assertNextRow(result, "2", "bbb", "2", "bbb"), () -> assertFalse(result.next()));
    }

    @Test
    void testInnerJoinWithProjection() throws SQLException {
        final String query = "SELECT b.y || " + VIRTUAL_SCHEMA_JDBC + ".t1.y FROM  " + VIRTUAL_SCHEMA_JDBC
                + ".t1 INNER JOIN  " + VIRTUAL_SCHEMA_JDBC + ".t2 b ON " + VIRTUAL_SCHEMA_JDBC + ".t1.x=b.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> assertNextRow(result, "bbbbbb"), () -> assertFalse(result.next()));
    }

    @Test
    void testLeftJoin() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA_ORA + ".t1 a LEFT OUTER JOIN  " + VIRTUAL_SCHEMA_ORA
                + ".t2 b ON a.x=b.x ORDER BY a.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> assertNextRow(result, "1", "aaa", null, null),
                () -> assertNextRow(result, "2", "bbb", "2", "bbb"), () -> assertFalse(result.next()));
    }

    @Test
    void testRightJoin() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA_JDBC + ".t1 a RIGHT OUTER JOIN  " + VIRTUAL_SCHEMA_JDBC
                + ".t2 b ON a.x=b.x ORDER BY a.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> assertNextRow(result, "2", "bbb", "2", "bbb"),
                () -> assertNextRow(result, null, null, "3", "ccc"), () -> assertFalse(result.next()));
    }

    @Test
    void testFullOuterJoin() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA_ORA + ".t1 a FULL OUTER JOIN  " + VIRTUAL_SCHEMA_ORA
                + ".t2 b ON a.x=b.x ORDER BY a.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> assertNextRow(result, "1", "aaa", null, null),
                () -> assertNextRow(result, "2", "bbb", "2", "bbb"),
                () -> assertNextRow(result, null, null, "3", "ccc"), () -> assertFalse(result.next()));
    }

    @Test
    void testRightJoinWithComplexCondition() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA_JDBC + ".t1 a RIGHT OUTER JOIN  " + VIRTUAL_SCHEMA_JDBC
                + ".t2 b ON a.x||a.y=b.x||b.y ORDER BY a.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> assertNextRow(result, "2", "bbb", "2", "bbb"),
                () -> assertNextRow(result, null, null, "3", "ccc"), () -> assertFalse(result.next()));
    }

    @Test
    void testFullOuterJoinWithComplexCondition() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA_ORA + ".t1 a FULL OUTER JOIN  " + VIRTUAL_SCHEMA_ORA
                + ".t2 b ON a.x-b.x=0 ORDER BY a.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> assertNextRow(result, "1", "aaa", null, null),
                () -> assertNextRow(result, "2", "bbb", "2", "bbb"),
                () -> assertNextRow(result, null, null, "3", "ccc"), () -> assertFalse(result.next()));
    }

    // Type Tests -------------------------------------------------------------
    @Test
    void testColumnTypeEquivalence() throws SQLException {
        final Map<String, String> jdbcColumnTypes = getColumnTypesOfTable(EXA_TABLE_JDBC);
        final Map<String, String> importColumnTypes = getColumnTypesOfTable(EXA_TABLE_ORA);

        for (final Map.Entry<String, String> entry : jdbcColumnTypes.entrySet()) {
            assertEquals(entry.getValue(), importColumnTypes.get(entry.getKey()));
        }
    }

    @Test
    void testSelectExpression() throws SQLException {
        final String query = "SELECT C7 + 1 FROM %s ORDER BY 1";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "12346.12345");
            matchNextRowDecimal(result, "12356.12345");
        }
        runMatchSingleRowExplain(query, "SELECT CAST((" + TYPE_TEST_T + ".\"C7\" + 1) AS FLOAT) FROM \"" + ORA_TABLE
                + "\" ORDER BY (" + TYPE_TEST_T + ".\"C7\" + 1)");
    }

    @Test
    void testFilterExpression() throws SQLException {
        final String query = "SELECT C7 FROM %s WHERE C7 > 12346";
        final ResultSet resultJDBC = runQueryJDBC(query);
        assertNextRow(resultJDBC, new BigDecimal("12355.12345"));
        final ResultSet resultORA = runQueryORA(query);
        assertNextRow(resultORA, "01.2355123450E4");

        runMatchSingleRowExplain(query, "SELECT " + TYPE_TEST_T + ".\"C7\" FROM \"" + ORA_TABLE + "\" WHERE 12346 < "
                + TYPE_TEST_T + ".\"C7\"");
    }

    @Test
    void testAggregateSingleGroup() throws SQLException {
        final String query = "SELECT min(C7) FROM %s";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "12345.12345");
        }
        runMatchSingleRowExplain(query,
                "SELECT CAST(MIN(" + TYPE_TEST_T + ".\"C7\") AS FLOAT) FROM \"" + ORA_TABLE + "\"");
    }

    @Test
    void testAggregateGroupByColumn() throws SQLException {
        final String query = "SELECT C5, min(C7) FROM %s GROUP BY C5 ORDER BY 1 DESC";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "123456789012345678901234567890123456", "12345.12345");
            matchNextRowDecimal(result, "1234567890.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query,
                "SELECT TO_CHAR(" + TYPE_TEST_T + ".\"C5\"), CAST(MIN(\"TYPE_TEST\".\"C7\") AS FLOAT) FROM \""
                        + ORA_TABLE + "\" GROUP BY " + TYPE_TEST_T + ".\"C5\" ORDER BY \"TYPE_TEST\".\"C5\" DESC");
    }

    @Test
    void testAggregateGroupByExpression() throws SQLException {
        final String query = "SELECT C5 + 1, min(C7) FROM %s GROUP BY C5 + 1 ORDER BY 1 DESC";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "123456789012345678901234567890123457", "12345.12345");
            matchNextRowDecimal(result, "1234567891.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query,
                "SELECT CAST((" + TYPE_TEST_T
                        + ".\"C5\" + 1) AS FLOAT), CAST(MIN(\"TYPE_TEST\".\"C7\") AS FLOAT) FROM \"" + ORA_TABLE
                        + "\" GROUP BY (" + TYPE_TEST_T + ".\"C5\" + 1) ORDER BY (\"TYPE_TEST\".\"C5\" + 1) DESC");
    }

    @Test
    void testAggregateGroupByTuple() throws SQLException {
        final String query = "SELECT C_NUMBER36, C5, min(C7) FROM %s GROUP BY C_NUMBER36, C5 ORDER BY C5 DESC";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "123456789012345678901234567890123456", "123456789012345678901234567890123456",
                    "12345.12345");
            matchNextRowDecimal(result, "123456789012345678901234567890123456", "1234567890.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query, "SELECT " + TYPE_TEST_T
                + ".\"C_NUMBER36\", TO_CHAR(\"TYPE_TEST\".\"C5\"), CAST(MIN(\"TYPE_TEST\".\"C7\") AS FLOAT) FROM \""
                + ORA_TABLE + "\" GROUP BY " + TYPE_TEST_T
                + ".\"C5\", \"TYPE_TEST\".\"C_NUMBER36\" ORDER BY \"TYPE_TEST\".\"C5\" DESC");
    }

    @Test
    void testAggregateHaving() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String query = "SELECT C5, min(C7) FROM %s GROUP BY C5 HAVING MIN(C7) > 12350";
        for (final ResultSet result : runQuery(query)) {
            matchNextRowDecimal(result, "1234567890.123456789", "12355.12345");
        }
        runMatchSingleRowExplain(query,
                "SELECT TO_CHAR(" + TYPE_TEST_T + ".\"C5\"), CAST(MIN(\"TYPE_TEST\".\"C7\") AS FLOAT) FROM \""
                        + ORA_TABLE + "\" GROUP BY " + TYPE_TEST_T
                        + ".\"C5\" HAVING 12350 < MIN(\"TYPE_TEST\".\"C7\")");
    }

    @Test
    void testOrderByColumn() throws SQLException {
        final String query = "SELECT C1 FROM %s ORDER BY C1 DESC NULLS LAST";
        for (final ResultSet result : runQuery(query)) {
            assertNextRow(result, "aaaaaaaaaaaaaaaaaaaa                              ");
            assertNextRow(result, (Object) null);
        }
        runMatchSingleRowExplain(query, "SELECT " + TYPE_TEST_T + ".\"C1\" FROM \"" + ORA_TABLE + "\" ORDER BY "
                + TYPE_TEST_T + ".\"C1\" DESC NULLS LAST");
    }

    @Test
    void testOrderByExpression() throws SQLException {
        final String query = "SELECT C7 FROM %s ORDER BY ABS(C7) DESC NULLS FIRST";
        final ResultSet resultJDBC = runQueryJDBC(query);
        assertNextRow(resultJDBC, new BigDecimal("12355.12345"));
        assertNextRow(resultJDBC, new BigDecimal("12345.12345"));
        final ResultSet resultORA = runQueryORA(query);
        assertNextRow(resultORA, "01.2355123450E4");
        assertNextRow(resultORA, "01.2345123450E4");

        runMatchSingleRowExplain(query, "SELECT " + TYPE_TEST_T + ".\"C7\" FROM \"" + ORA_TABLE + "\" ORDER BY ABS("
                + TYPE_TEST_T + ".\"C7\") DESC");
    }

    @Test
    void testLimit() throws SQLException {
        final String query = "SELECT C7 FROM %s ORDER BY C7 LIMIT 2";
        final ResultSet resultJDBC = runQueryJDBC(query);
        assertNextRow(resultJDBC, new BigDecimal("12345.12345"));
        assertNextRow(resultJDBC, new BigDecimal("12355.12345"));
        final ResultSet resultORA = runQueryORA(query);
        assertNextRow(resultORA, "01.2345123450E4");
        assertNextRow(resultORA, "01.2355123450E4");

        runMatchSingleRowExplain(query, "SELECT LIMIT_SUBSELECT.* FROM ( SELECT " + TYPE_TEST_T + ".\"C7\" FROM \""
                + ORA_TABLE + "\" ORDER BY " + TYPE_TEST_T + ".\"C7\"  ) LIMIT_SUBSELECT WHERE ROWNUM <= 2");
    }

    @Test
    void testLimitOffset() throws SQLException {
        final String query = "SELECT C7 FROM %s ORDER BY C7 LIMIT 1 OFFSET 1";
        final ResultSet resultJDBC = runQueryJDBC(query);
        assertNextRow(resultJDBC, new BigDecimal("12355.12345"));
        final ResultSet resultORA = runQueryORA(query);
        assertNextRow(resultORA, "01.2355123450E4");

        runMatchSingleRowExplain(query,
                "SELECT c0 FROM ( SELECT LIMIT_SUBSELECT.*, ROWNUM ROWNUM_SUB FROM ( SELECT " + TYPE_TEST_T
                        + ".\"C7\" AS c0 FROM \"" + ORA_TABLE + "\" ORDER BY " + TYPE_TEST_T
                        + ".\"C7\"  ) LIMIT_SUBSELECT WHERE ROWNUM <= 2 ) WHERE ROWNUM_SUB > 1");
    }

    @Test
    void testChar() throws SQLException {
        final String query = "SELECT C1 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            assertNextRow(result, "aaaaaaaaaaaaaaaaaaaa                              ");
        }
        assertEquals("CHAR(50) ASCII", getColumnType("C1"));
    }

    @Test
    void testNChar() throws SQLException {
        final String query = "SELECT C2 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            assertNextRow(result, "bbbbbbbbbbbbbbbbbbbb                              ");
        }
        assertEquals("CHAR(50) UTF8", getColumnType("C2"));
    }

    @Test
    void testVarchar() throws SQLException {
        final String query = "SELECT C3 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            assertNextRow(result, "cccccccccccccccccccc");
        }
        assertEquals("VARCHAR(50) ASCII", getColumnType("C3"));
    }

    @Test
    void testNVarchar() throws SQLException {
        final String query = "SELECT C4 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            assertNextRow(result, "dddddddddddddddddddd");
        }
        assertEquals("VARCHAR(50) UTF8", getColumnType("C4"));
    }

    @Test
    void testNumber() throws SQLException {
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
    void testNumber36() throws SQLException {
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
    void testNumber38() throws SQLException {
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
    void testNumber10S5() throws SQLException {
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
    void testBinaryFloat() throws SQLException {
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
    void testBinaryDouble() throws SQLException {
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
    void testFloat() throws SQLException {
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
    void testFloat126() throws SQLException {
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
    void testLong() throws SQLException {
        final String query = "SELECT C_LONG FROM " + EXA_TABLE_JDBC;
        final ResultSet result = executeQuery(query);
        assertNextRow(result, "test long 123");
        assertEquals("VARCHAR(2000000) ASCII", getColumnType("C_LONG"));
    }

    @Test
    void testDate() throws SQLException {
        final String query = "SELECT C10 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            assertNextRow(result, java.sql.Date.valueOf("2016-08-19"));
        }
        runMatchSingleRowExplain(query, "SELECT " + TYPE_TEST_T + ".\"C10\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("TIMESTAMP", getColumnType("C10"));
    }

    @Test
    void testTimestamp3() throws SQLException {
        final String query = "SELECT C11 FROM %s";
        final ResultSet resultJDBC = runQueryJDBC(query);
        assertNextRow(resultJDBC, Timestamp.valueOf("2013-03-11 17:30:15.123"));
        final ResultSet resultORA = runQueryORA(query);
        assertNextRow(resultORA, Timestamp.valueOf("2013-03-11 17:30:15.123"));

        runMatchSingleRowExplainJDBC(query, "SELECT TO_TIMESTAMP(TO_CHAR(" + TYPE_TEST_T
                + ".\"C11\", 'YYYY-MM-DD HH24:MI:SS.FF3'), 'YYYY-MM-DD HH24:MI:SS.FF3') FROM \"" + ORA_TABLE + "\"");
        runMatchSingleRowExplainORA(query, "SELECT " + TYPE_TEST_T + ".\"C11\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("TIMESTAMP", getColumnTypeJDBC("C11"));
        assertEquals("TIMESTAMP", getColumnTypeORA("C11"));
    }

    @Test
    void testTimestamp6() throws SQLException {
        final String query = "SELECT C12 FROM %s";
        final ResultSet resultJDBC = runQueryJDBC(query);
        assertNextRow(resultJDBC, Timestamp.valueOf("2013-03-11 17:30:15.123"));
        final ResultSet resultORA = runQueryORA(query);
        assertNextRow(resultORA, Timestamp.valueOf("2013-03-11 17:30:15.123"));

        runMatchSingleRowExplainJDBC(query, "SELECT TO_TIMESTAMP(TO_CHAR(" + TYPE_TEST_T
                + ".\"C12\", 'YYYY-MM-DD HH24:MI:SS.FF3'), 'YYYY-MM-DD HH24:MI:SS.FF3') FROM \"" + ORA_TABLE + "\"");
        runMatchSingleRowExplainORA(query, "SELECT " + TYPE_TEST_T + ".\"C12\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("TIMESTAMP", getColumnTypeJDBC("C12"));
        assertEquals("TIMESTAMP", getColumnTypeORA("C12"));
    }

    @Test
    void testTimestamp9() throws SQLException {
        final String query = "SELECT C13 FROM %s";
        final ResultSet resultJDBC = runQueryJDBC(query);
        assertNextRow(resultJDBC, Timestamp.valueOf("2013-03-11 17:30:15.123"));
        final ResultSet resultORA = runQueryORA(query);
        assertNextRow(resultORA, Timestamp.valueOf("2013-03-11 17:30:15.123"));

        runMatchSingleRowExplainJDBC(query, "SELECT TO_TIMESTAMP(TO_CHAR(" + TYPE_TEST_T
                + ".\"C13\", 'YYYY-MM-DD HH24:MI:SS.FF3'), 'YYYY-MM-DD HH24:MI:SS.FF3') FROM \"" + ORA_TABLE + "\"");
        runMatchSingleRowExplainORA(query, "SELECT " + TYPE_TEST_T + ".\"C13\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("TIMESTAMP", getColumnTypeJDBC("C13"));
        assertEquals("TIMESTAMP", getColumnTypeORA("C13"));
    }

    @Test
    void testTimestampTZ() throws SQLException {
        final String query = "SELECT C14 FROM %s";
        final ResultSet resultJDBC = runQueryJDBC(query);
        assertNextRow(resultJDBC, Timestamp.valueOf("2016-08-19 11:28:05.0"));
        final ResultSet resultORA = runQueryORA(query);
        assertNextRow(resultORA, Timestamp.valueOf("2016-08-19 19:28:05.000"));

        runMatchSingleRowExplainJDBC(query, "SELECT TO_TIMESTAMP(TO_CHAR(" + TYPE_TEST_T
                + ".\"C14\", 'YYYY-MM-DD HH24:MI:SS.FF3'), 'YYYY-MM-DD HH24:MI:SS.FF3') FROM \"" + ORA_TABLE + "\"");
        runMatchSingleRowExplainORA(query, "SELECT " + TYPE_TEST_T + ".\"C14\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("TIMESTAMP", getColumnTypeJDBC("C14"));
        assertEquals("TIMESTAMP", getColumnTypeORA("C14"));
    }

    @Test
    void testTimestampLocalTZ() throws SQLException {
        executeUpdate("ALTER SESSION SET TIME_ZONE = 'UTC'");
        final String query = "SELECT C15 FROM %s";
        final ResultSet resultJDBC = runQueryJDBC(query);
        assertNextRow(resultJDBC, Timestamp.valueOf("2018-04-30 19:00:05.0"));
        final ResultSet resultORA = runQueryORA(query);
        assertNextRow(resultORA, Timestamp.valueOf("2018-04-30 18:00:05.000"));

        runMatchSingleRowExplainJDBC(query, "SELECT TO_TIMESTAMP(TO_CHAR(" + TYPE_TEST_T
                + ".\"C15\", 'YYYY-MM-DD HH24:MI:SS.FF3'), 'YYYY-MM-DD HH24:MI:SS.FF3') FROM \"" + ORA_TABLE + "\"");
        runMatchSingleRowExplainORA(query, "SELECT " + TYPE_TEST_T + ".\"C15\" FROM \"" + ORA_TABLE + "\"");
        assertEquals("TIMESTAMP", getColumnTypeJDBC("C15"));
        assertEquals("TIMESTAMP", getColumnTypeORA("C15"));
    }

    @Test
    void testIntervalYear() throws SQLException {
        final String query = "SELECT C16 FROM %s";
        for (final ResultSet result : runQuery(query)) {
            assertNextRow(result, "+54-02");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(" + TYPE_TEST_T + ".\"C16\") FROM \"" + ORA_TABLE + "\"");
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C16"));
    }

    @Test
    void testIntervalDay() throws SQLException {
        final String query = "SELECT C17 FROM %s ORDER BY 1";
        for (final ResultSet result : runQuery(query)) {
            assertNextRow(result, "+01 11:12:10.123000");
            assertNextRow(result, "+02 02:03:04.123456");
        }
        runMatchSingleRowExplain(query, "SELECT TO_CHAR(" + TYPE_TEST_T + ".\"C17\") FROM \"" + ORA_TABLE
                + "\" ORDER BY " + TYPE_TEST_T + ".\"C17\"");
        assertEquals("VARCHAR(2000000) UTF8", getColumnType("C17"));
    }

    @Test
    void testSelectAllTimestampColumns() throws SQLException {
        executeUpdate("ALTER SESSION SET TIME_ZONE = 'UTC'");
        final String query = "SELECT * FROM %s.%s";
        final ResultSet resultJDBC = runQueryJDBC(String.format(query, VIRTUAL_SCHEMA_JDBC, "TS_T"));
        assertNextRow(resultJDBC, Timestamp.valueOf("2018-01-01 11:00:00.0"),
                Timestamp.valueOf("2018-01-01 11:00:00.0"), Timestamp.valueOf("2018-01-01 11:00:00.0"));
        final ResultSet resultORA = runQueryORA(String.format(query, VIRTUAL_SCHEMA_ORA, "TS_T"));
        assertNextRow(resultORA, Timestamp.valueOf("2018-01-01 11:00:00.000"),
                Timestamp.valueOf("2018-01-01 10:00:00.000"), Timestamp.valueOf("2018-01-01 10:00:00.000"));
        final Map<String, String> columnTypesJDBC = getColumnTypesOfTable(VIRTUAL_SCHEMA_JDBC + ".TS_T");
        final Map<String, String> columnTypesORA = getColumnTypesOfTable(VIRTUAL_SCHEMA_ORA + ".TS_T");
        assertAll(() -> assertThat(columnTypesJDBC.get("A"), equalTo("TIMESTAMP")),
                () -> assertThat(columnTypesORA.get("A"), equalTo("TIMESTAMP")),
                () -> assertThat(columnTypesJDBC.get("B"), equalTo("TIMESTAMP")),
                () -> assertThat(columnTypesORA.get("B"), equalTo("TIMESTAMP")),
                () -> assertThat(columnTypesJDBC.get("C"), equalTo("TIMESTAMP")),
                () -> assertThat(columnTypesORA.get("C"), equalTo(("TIMESTAMP"))));
    }
}