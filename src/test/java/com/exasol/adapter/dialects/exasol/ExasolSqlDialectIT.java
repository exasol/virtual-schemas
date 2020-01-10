package com.exasol.adapter.dialects.exasol;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import com.exasol.adapter.dialects.IntegrationTestConfigurationCondition;

/**
 * Integration tests for the Exasol SQL dialect.
 */
@Tag("integration")
@ExtendWith(IntegrationTestConfigurationCondition.class)
class ExasolSqlDialectIT extends AbstractIntegrationTest {

    public static class ConnectionBuilder {
        private final String connectionName;
        private final String connectionString;
        private String connectionUser;
        private String connectionPassword;

        ConnectionBuilder(final String connectionName, final String connectionString) {
            this.connectionName = connectionName;
            this.connectionString = connectionString;
            this.connectionUser = "";
            this.connectionPassword = "";
        }

        ConnectionBuilder user(final String user) {
            this.connectionUser = user;
            return this;
        }

        ConnectionBuilder password(final String password) {
            this.connectionPassword = password;
            return this;
        }

        String getCreateConnection() {
            final StringBuilder createConnection = new StringBuilder();
            createConnection.append("CREATE CONNECTION ");
            createConnection.append(this.connectionName);
            createConnection.append(" TO '");
            createConnection.append(this.connectionString);
            createConnection.append("'");
            if ((this.connectionUser != "") && (this.connectionPassword != "")) {
                createConnection.append(" USER '");
                createConnection.append(this.connectionUser);
                createConnection.append("' IDENTIFIED BY '");
                createConnection.append(this.connectionPassword);
                createConnection.append("'");
            }
            return createConnection.toString();
        }
    }

    private static final String TEST_SCHEMA = "NATIVE_EXA_IT";
    private static final String TEST_SCHEMA_MIXED_CASE = "NATIVE_EXA_IT_Mixed_Case";
    private static final String VIRTUAL_SCHEMA = "VS_EXA_IT";
    private static final String VIRTUAL_SCHEMA_MIXED_CASE = "VS_EXA_IT_MIXED_CASE";
    private static final String VIRTUAL_SCHEMA_JDBC = "VS_EXA_IT_JDBC";
    private static final boolean IS_LOCAL = true;

    @BeforeAll
    static void beforeAll() throws FileNotFoundException, SQLException, ClassNotFoundException {
        assumeTrue(getConfig().exasolTestsRequested());
        setConnection(connectToExa());
        final String connectionString = "jdbc:exa:localhost:" + getPortOfConnectedDatabase(); // connect via Virtual
                                                                                              // Schema to local
                                                                                              // database
        // The Exasol JDBC driver is included in the Maven dependencies, so no need to
        // add it.
        final List<String> includes = List.of(getConfig().getJdbcAdapterPath());
        createJDBCAdapter(includes);
        createTestSchema();
        createVirtualSchema(VIRTUAL_SCHEMA, ExasolSqlDialect.NAME, "", TEST_SCHEMA, "", getConfig().getExasolUser(),
                getConfig().getExasolPassword(), "ADAPTER.JDBC_ADAPTER", connectionString, IS_LOCAL,
                getConfig().debugAddress(), "", null, "");
        createVirtualSchema(VIRTUAL_SCHEMA_MIXED_CASE, ExasolSqlDialect.NAME, "", TEST_SCHEMA_MIXED_CASE, "",
                getConfig().getExasolUser(), getConfig().getExasolPassword(), "ADAPTER.JDBC_ADAPTER", connectionString,
                IS_LOCAL, getConfig().debugAddress(), "", null, "");
        createVirtualSchema(VIRTUAL_SCHEMA_JDBC, ExasolSqlDialect.NAME, "", TEST_SCHEMA, "",
                getConfig().getExasolUser(), getConfig().getExasolPassword(), "ADAPTER.JDBC_ADAPTER", connectionString,
                false, getConfig().debugAddress(), "", null, "");
    }

    private static void createTestSchema() throws SQLException {
        // Exasol integration test is special, because we can directly create our test
        // data.
        // For other dialects you have to prepare the source data base separately,
        // because
        // otherwise we would need to make the JDBC driver visible to the integration
        // test framework as well (adds complexity)
        final Statement stmt = getConnection().createStatement();
        stmt.execute("DROP SCHEMA IF EXISTS " + TEST_SCHEMA + " CASCADE");
        stmt.execute("CREATE SCHEMA " + TEST_SCHEMA);
        stmt.execute("CREATE TABLE ALL_EXA_TYPES (" + " c1 varchar(100) default 'bar',"
                + " c2 varchar(100) CHARACTER SET ASCII default 'bar'," + " c3 char(10) default 'foo',"
                + " c4 char(10) CHARACTER SET ASCII default 'bar'," + " c5 decimal(5,0) default 1,"
                + " c6 decimal(6,3) default 1.2," + " c7 double default 1E2," + " c8 boolean default TRUE,"
                + " c9 date default '2016-06-01'," + " c10 timestamp default '2016-06-01 00:00:01.000',"
                + " c11 timestamp with local time zone default '2016-06-01 00:00:02.000',"
                + " c12 interval year to month default '3-5'," + " c13 interval day to second default '2 12:50:10.123',"
                + " c14 geometry(3857) default 'POINT(2 5)'" + ")");

        stmt.execute("INSERT INTO " + TEST_SCHEMA + ".ALL_EXA_TYPES VALUES(" + "'a茶'," + "'b'," + "'c茶'," + "'d',"
                + "123," + "123.456," + "2.2," + "FALSE," + "'2016-08-01'," + "'2016-08-01 00:00:01.000',"
                + "'2016-08-01 00:00:02.000'," + "'4-6'," + "'3 12:50:10.123'," + "'POINT(2 5)'" + ");");

        stmt.execute("CREATE TABLE WITH_NULLS (c1 int, c2 varchar(100))");
        stmt.execute("INSERT INTO WITH_NULLS VALUES " + " (1, 'a')," + " (2, null)," + " (3, 'b')," + " (1, null),"
                + " (null, 'c')");
        stmt.execute("CREATE TABLE SIMPLE_VALUES (a int, b varchar(100), c double)");
        stmt.execute("INSERT INTO SIMPLE_VALUES VALUES " + " (1, 'a', 1.1)," + " (2, 'b', 2.2)," + " (3, 'c', 3.3),"
                + " (1, 'd', 4.4)," + " (2, 'e', 5.5)," + " (3, 'f', 6.6)," + " (null, null, null)");

        stmt.execute(String.format("create table %s.t1(x int, y varchar(100))", TEST_SCHEMA));
        stmt.execute(String.format("insert into %s.t1 values (1,'aaa'), (2,'bbb');", TEST_SCHEMA));
        stmt.execute(String.format("create table %s.t2(x int, y varchar(100))", TEST_SCHEMA));
        stmt.execute(String.format("insert into %s.t2 values (2,'bbb'), (3,'ccc');", TEST_SCHEMA));

        // Create schema, table and column with mixed case identifiers (to test correct
        // mapping, and correct sql generation of adapter)
        stmt.execute("DROP SCHEMA IF EXISTS \"" + TEST_SCHEMA_MIXED_CASE + "\" CASCADE");
        stmt.execute("CREATE SCHEMA \"" + TEST_SCHEMA_MIXED_CASE + "\"");
        stmt.execute("CREATE TABLE \"Table_Mixed_Case\" (\"Column1\" int, \"column2\" int, COLUMN3 int)");
        stmt.execute("INSERT INTO \"Table_Mixed_Case\" VALUES (1, 2, 3)");
    }

    @Test
    void testDataTypeMapping() throws SQLException {
        final ResultSet result = executeQuery(
                "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, COLUMN_DEFAULT FROM EXA_DBA_COLUMNS WHERE COLUMN_SCHEMA = '"
                        + VIRTUAL_SCHEMA + "' AND COLUMN_TABLE='ALL_EXA_TYPES' ORDER BY COLUMN_ORDINAL_POSITION");
        assertNextRow(result, "C1", "VARCHAR(100) UTF8", (long) 100, null, null, "'bar'");
        assertNextRow(result, "C2", "VARCHAR(100) ASCII", (long) 100, null, null, "'bar'");
        assertNextRow(result, "C3", "CHAR(10) UTF8", (long) 10, null, null, "'foo'");
        assertNextRow(result, "C4", "CHAR(10) ASCII", (long) 10, null, null, "'bar'");
        assertNextRow(result, "C5", "DECIMAL(5,0)", (long) 5, (long) 5, (long) 0, "1");
        assertNextRow(result, "C6", "DECIMAL(6,3)", (long) 6, (long) 6, (long) 3, "1.2");
        assertNextRow(result, "C7", "DOUBLE", (long) 64, null, null, "100");
        assertNextRow(result, "C8", "BOOLEAN", (long) 1, null, null, "TRUE");
        assertNextRow(result, "C9", "DATE", (long) 10, null, null, "'2016-06-01'");
        assertNextRow(result, "C10", "TIMESTAMP", (long) 29, null, null, "'2016-06-01 00:00:01.000'");
        assertNextRow(result, "C11", "TIMESTAMP WITH LOCAL TIME ZONE", (long) 29, null, null,
                "'2016-06-01 00:00:02.000'");
        assertNextRow(result, "C12", "INTERVAL YEAR(2) TO MONTH", (long) 13, null, null, "'3-5'");
        assertNextRow(result, "C13", "INTERVAL DAY(2) TO SECOND(3)", (long) 29, null, null, "'2 12:50:10.123'");
        assertLastRow(result, "C14", "GEOMETRY(3857)", (long) 8000000, null, null, "'POINT(2 5)'"); // srid not yet
                                                                                                    // supported, so
                                                                                                    // will
                                                                                                    // always default to
                                                                                                    // 3857
    }

    @Test
    void testDataTypeSelect() throws SQLException {
        final ResultSet result = executeQuery("SELECT * FROM " + VIRTUAL_SCHEMA + ".ALL_EXA_TYPES");
        assertNextRow(result, "a茶", "b", "c茶        ", "d         ", 123, new BigDecimal("123.456"), 2.2, false,
                getSqlDate(2016, 8, 1), getSqlTimestamp(2016, 8, 1, 0, 0, 1, 0),
                getSqlTimestamp(2016, 8, 1, 0, 0, 2, 0), "+04-06", "+03 12:50:10.123", "POINT (2 5)");
    }

    @Test
    void testIdentifierCaseSensitivityOnTable() throws SQLException {
        final ResultSet result = executeQuery("SELECT * FROM " + VIRTUAL_SCHEMA_MIXED_CASE + ".\"Table_Mixed_Case\"");
        assertLastRow(result, 1L, 2L, 3L);
    }

    @Test
    void testIdentifierCaseSensitivityOnColumns() throws SQLException {
        final ResultSet result = executeQuery(
                "SELECT \"Column1\", \"column2\", COLUMN3 FROM " + VIRTUAL_SCHEMA_MIXED_CASE + ".\"Table_Mixed_Case\"");
        assertLastRow(result, 1L, 2L, 3L);
    }

    @Test
    void assertUnquotedMixedCaseTableIsNotFound() throws SQLException {
        assertThrows(
                SQLException.class, () -> executeQuery("SELECT \"Column1\", \"column2\", COLUMN3 FROM "
                        + VIRTUAL_SCHEMA_MIXED_CASE + ".Table_Mixed_Case"),
                "object VS_EXA_IT_MIXED_CASE.TABLE_MIXED_CASE not found");
    }

    @Test
    void assertUnquotedMixedCaseColumnIsNotFound() throws SQLException {
        assertThrows(SQLException.class,
                () -> executeQuery(
                        "SELECT Column1, column2, COLUMN3 FROM " + VIRTUAL_SCHEMA_MIXED_CASE + ".\"Table_Mixed_Case\""),
                "object COLUMN1 not found");
    }

    @Test
    void testGroupConcat() throws SQLException {
        String query = "SELECT GROUP_CONCAT(A) FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        ResultSet result = executeQuery(query);
        assertLastRow(result, "1,1,2,2,3,3");
        matchSingleRowExplain(query,
                "SELECT GROUP_CONCAT(\"SIMPLE_VALUES\".\"A\") FROM \"" + TEST_SCHEMA + "\".\"SIMPLE_VALUES\"",
                IS_LOCAL);
        query = "SELECT GROUP_CONCAT(DISTINCT A) FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        result = executeQuery(query);
        assertLastRow(result, "1,2,3");
        matchSingleRowExplain(query,
                "SELECT GROUP_CONCAT(DISTINCT \"SIMPLE_VALUES\".\"A\") FROM \"" + TEST_SCHEMA + "\".\"SIMPLE_VALUES\"",
                IS_LOCAL);
        query = "SELECT GROUP_CONCAT(A ORDER BY C) FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        result = executeQuery(query);
        assertLastRow(result, "1,2,3,1,2,3");
        matchSingleRowExplain(query,
                "SELECT GROUP_CONCAT(\"SIMPLE_VALUES\".\"A\" ORDER BY \"SIMPLE_VALUES\".\"C\") FROM \"" + TEST_SCHEMA
                        + "\".\"SIMPLE_VALUES\"",
                IS_LOCAL);
        query = "SELECT GROUP_CONCAT(A ORDER BY C DESC) FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        result = executeQuery(query);
        assertLastRow(result, "3,2,1,3,2,1");
        matchSingleRowExplain(query,
                "SELECT GROUP_CONCAT(\"SIMPLE_VALUES\".\"A\" ORDER BY \"SIMPLE_VALUES\".\"C\" DESC) FROM \""
                        + TEST_SCHEMA + "\".\"SIMPLE_VALUES\"",
                IS_LOCAL);
        query = "SELECT GROUP_CONCAT(A ORDER BY C DESC NULLS LAST) FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        result = executeQuery(query);
        assertLastRow(result, "3,2,1,3,2,1");
        matchSingleRowExplain(query,
                "SELECT GROUP_CONCAT(\"SIMPLE_VALUES\".\"A\" ORDER BY \"SIMPLE_VALUES\".\"C\" DESC NULLS LAST) FROM \""
                        + TEST_SCHEMA + "\".\"SIMPLE_VALUES\"",
                IS_LOCAL);
        query = "SELECT GROUP_CONCAT(A SEPARATOR ';'||' ')  FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        result = executeQuery(query);
        assertLastRow(result, "1; 1; 2; 2; 3; 3");
        matchSingleRowExplain(query, "SELECT GROUP_CONCAT(\"SIMPLE_VALUES\".\"A\" SEPARATOR '; ') FROM \"" + TEST_SCHEMA
                + "\".\"SIMPLE_VALUES\"", IS_LOCAL);
    }

    @Test
    void testExtract() throws SQLException {
        String query = "SELECT EXTRACT(MONTH FROM C9) FROM " + VIRTUAL_SCHEMA + ".ALL_EXA_TYPES";
        ResultSet result = executeQuery(query);
        assertLastRow(result, (short) 8);
        matchSingleRowExplain(query,
                "SELECT EXTRACT(MONTH FROM \"ALL_EXA_TYPES\".\"C9\") FROM \"" + TEST_SCHEMA + "\".\"ALL_EXA_TYPES\"",
                IS_LOCAL);
        query = "SELECT EXTRACT(MONTH FROM C12) FROM " + VIRTUAL_SCHEMA + ".ALL_EXA_TYPES";
        result = executeQuery(query);
        assertLastRow(result, (short) 6);
        matchSingleRowExplain(query,
                "SELECT EXTRACT(MONTH FROM \"ALL_EXA_TYPES\".\"C12\") FROM \"" + TEST_SCHEMA + "\".\"ALL_EXA_TYPES\"",
                IS_LOCAL);
    }

    @Test
    void testCast() throws SQLException {
        String query = "SELECT CAST(A AS CHAR(15)) FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        ResultSet result = executeQuery(query);
        assertNextRow(result, "1              ");
        matchSingleRowExplain(query,
                "SELECT CAST(\"SIMPLE_VALUES\".\"A\" AS CHAR(15) UTF8) FROM \"" + TEST_SCHEMA + "\".\"SIMPLE_VALUES\"",
                IS_LOCAL);
        query = "SELECT CAST(CAST(A > 0 AS VARCHAR(15)) AS BOOLEAN) FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        result = executeQuery(query);
        assertNextRow(result, true);
        matchSingleRowExplain(query,
                "SELECT CAST(CAST(0 < \"SIMPLE_VALUES\".\"A\" AS VARCHAR(15) UTF8) AS BOOLEAN) FROM \"" + TEST_SCHEMA
                        + "\".\"SIMPLE_VALUES\"",
                IS_LOCAL);
        query = "SELECT CAST(CAST(C9 AS VARCHAR(30)) AS DATE) FROM " + VIRTUAL_SCHEMA + ".ALL_EXA_TYPES";
        result = executeQuery(query);
        assertNextRow(result, getSqlDate(2016, 8, 1));
        matchSingleRowExplain(query, "SELECT CAST(CAST(\"ALL_EXA_TYPES\".\"C9\" AS VARCHAR(30) UTF8) AS DATE) FROM \""
                + TEST_SCHEMA + "\".\"ALL_EXA_TYPES\"", IS_LOCAL);
        query = "SELECT CAST(CAST(A AS VARCHAR(15)) AS DECIMAL(8, 1)) FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        result = executeQuery(query);
        assertNextRow(result, new BigDecimal("1.0"));
        matchSingleRowExplain(query,
                "SELECT CAST(CAST(\"SIMPLE_VALUES\".\"A\" AS VARCHAR(15) UTF8) AS DECIMAL(8, 1)) FROM \"" + TEST_SCHEMA
                        + "\".\"SIMPLE_VALUES\"",
                IS_LOCAL);
        query = "SELECT CAST(CAST(C AS VARCHAR(15)) AS DOUBLE) FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        result = executeQuery(query);
        assertNextRow(result, 1.1d);
        matchSingleRowExplain(query, "SELECT CAST(CAST(\"SIMPLE_VALUES\".\"C\" AS VARCHAR(15) UTF8) AS DOUBLE) FROM \""
                + TEST_SCHEMA + "\".\"SIMPLE_VALUES\"", IS_LOCAL);
        query = "SELECT CAST(CAST(C14 AS VARCHAR(100)) AS GEOMETRY(5)) FROM " + VIRTUAL_SCHEMA + ".ALL_EXA_TYPES";
        result = executeQuery(query);
        assertNextRow(result, "POINT (2 5)");
        matchSingleRowExplain(query,
                "SELECT CAST(CAST(\"ALL_EXA_TYPES\".\"C14\" AS VARCHAR(100) UTF8) AS GEOMETRY(5)) FROM \"" + TEST_SCHEMA
                        + "\".\"ALL_EXA_TYPES\"",
                IS_LOCAL);
        query = "SELECT CAST(CAST(C13 AS VARCHAR(100)) AS INTERVAL DAY (5) TO SECOND (2)) FROM " + VIRTUAL_SCHEMA
                + ".ALL_EXA_TYPES";
        result = executeQuery(query);
        assertNextRow(result, "+00003 12:50:10.12");
        matchSingleRowExplain(query,
                "SELECT CAST(CAST(\"ALL_EXA_TYPES\".\"C13\" AS VARCHAR(100) UTF8) AS INTERVAL DAY (5) TO SECOND (2)) FROM \""
                        + TEST_SCHEMA + "\".\"ALL_EXA_TYPES\"",
                IS_LOCAL);
        query = "SELECT CAST(CAST(C12 AS VARCHAR(100)) AS INTERVAL YEAR (5) TO MONTH) FROM " + VIRTUAL_SCHEMA
                + ".ALL_EXA_TYPES";
        result = executeQuery(query);
        assertNextRow(result, "+00004-06");
        matchSingleRowExplain(query,
                "SELECT CAST(CAST(\"ALL_EXA_TYPES\".\"C12\" AS VARCHAR(100) UTF8) AS INTERVAL YEAR (5) TO MONTH) FROM \""
                        + TEST_SCHEMA + "\".\"ALL_EXA_TYPES\"",
                IS_LOCAL);
        query = "SELECT CAST(CAST(C10 AS VARCHAR(100)) AS TIMESTAMP) FROM " + VIRTUAL_SCHEMA + ".ALL_EXA_TYPES";
        result = executeQuery(query);
        assertNextRow(result, getSqlTimestamp(2016, 8, 1, 0, 0, 1, 0));
        matchSingleRowExplain(query,
                "SELECT CAST(CAST(\"ALL_EXA_TYPES\".\"C10\" AS VARCHAR(100) UTF8) AS TIMESTAMP) FROM \"" + TEST_SCHEMA
                        + "\".\"ALL_EXA_TYPES\"",
                IS_LOCAL);
        query = "SELECT CAST(CAST(C11 AS VARCHAR(100)) AS TIMESTAMP WITH LOCAL TIME ZONE) FROM " + VIRTUAL_SCHEMA
                + ".ALL_EXA_TYPES";
        result = executeQuery(query);
        assertNextRow(result, getSqlTimestamp(2016, 8, 1, 0, 0, 2, 0));
        matchSingleRowExplain(query,
                "SELECT CAST(CAST(\"ALL_EXA_TYPES\".\"C11\" AS VARCHAR(100) UTF8) AS TIMESTAMP WITH LOCAL TIME ZONE) FROM \""
                        + TEST_SCHEMA + "\".\"ALL_EXA_TYPES\"",
                IS_LOCAL);
        query = "SELECT CAST(A AS VARCHAR(15)) FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        result = executeQuery(query);
        assertNextRow(result, "1");
        matchSingleRowExplain(query, "SELECT CAST(\"SIMPLE_VALUES\".\"A\" AS VARCHAR(15) UTF8) FROM \"" + TEST_SCHEMA
                + "\".\"SIMPLE_VALUES\"", IS_LOCAL);
    }

    @Test
    void testCase() throws SQLException {
        String query = "SELECT CASE A WHEN 1 THEN 'YES' WHEN 2 THEN 'PERHAPS' ELSE 'NO' END FROM " + VIRTUAL_SCHEMA
                + ".SIMPLE_VALUES";
        ResultSet result = executeQuery(query);
        assertNextRow(result, "YES");
        matchSingleRowExplain(query,
                "SELECT CASE \"SIMPLE_VALUES\".\"A\" WHEN 1 THEN 'YES' WHEN 2 THEN 'PERHAPS' ELSE 'NO' END FROM \""
                        + TEST_SCHEMA + "\".\"SIMPLE_VALUES\"",
                IS_LOCAL);
        query = "SELECT CASE WHEN A > 1 THEN 'YES' ELSE 'NO' END FROM " + VIRTUAL_SCHEMA + ".SIMPLE_VALUES";
        result = executeQuery(query);
        assertNextRow(result, "NO");
        matchSingleRowExplain(query, "SELECT CASE WHEN 1 < \"SIMPLE_VALUES\".\"A\" THEN 'YES' ELSE 'NO' END FROM \""
                + TEST_SCHEMA + "\".\"SIMPLE_VALUES\"", IS_LOCAL);
    }

    @Test
    void testErrorMessages() throws SQLException, FileNotFoundException {
        assertThrows(Exception.class,
                () -> createVirtualSchema("VS_EXA_IT_BROKEN", ExasolSqlDialect.NAME, "", "NATIVE_EXA_IT",
                        "NO_CONNECTION", "", "", "ADAPTER.JDBC_ADAPTER", "", false, getConfig().debugAddress(), "",
                        null, ""),
                "Could not access the connection information of connection NO_CONNECTION");
    }

    @Test
    void testVirtualSchemaImportFromJDBCWithConnectionName() throws SQLException, FileNotFoundException {
        final String connectionString = "jdbc:exa:localhost:" + getPortOfConnectedDatabase();
        final ConnectionBuilder JDBCConnection = new ConnectionBuilder("VS_JDBC_WITH_CONNNAME_CONNECTION",
                connectionString).user(getConfig().getExasolUser()).password(getConfig().getExasolPassword());
        execute(JDBCConnection.getCreateConnection());
        createVirtualSchema("VS_JDBC_WITH_CONNNAME", ExasolSqlDialect.NAME, "", TEST_SCHEMA,
                "VS_JDBC_WITH_CONNNAME_CONNECTION", "", "", "ADAPTER.JDBC_ADAPTER", "", false, "", "", null, "");
        final String query = "SELECT 1 FROM VS_JDBC_WITH_CONNNAME.SIMPLE_VALUES";
        final ResultSet result = executeQuery(query);
        assertNextRow(result, Short.valueOf("1"));
        matchSingleRowExplain(query,
                "IMPORT INTO (c1 DECIMAL(1, 0)) FROM JDBC AT VS_JDBC_WITH_CONNNAME_CONNECTION STATEMENT 'SELECT 1 FROM \"NATIVE_EXA_IT\".\"SIMPLE_VALUES\"'",
                IS_LOCAL);
    }

    @Test
    void testVirtualSchemaImportFromEXAWithConnectionName() throws SQLException, FileNotFoundException {
        final String connectionString = "jdbc:exa:localhost:" + getPortOfConnectedDatabase();
        final ConnectionBuilder EXAConnection = new ConnectionBuilder("VS_EXA_WITH_CONNNAME_CONNECTION",
                connectionString).user(getConfig().getExasolUser()).password(getConfig().getExasolPassword());
        execute(EXAConnection.getCreateConnection());
        createVirtualSchema("VS_EXA_WITH_CONNNAME", ExasolSqlDialect.NAME, "", TEST_SCHEMA,
                "VS_EXA_WITH_CONNNAME_CONNECTION", "", "", "ADAPTER.JDBC_ADAPTER", "", false, "", "",
                "IMPORT_FROM_EXA = 'true' EXA_CONNECTION_STRING = 'localhost:" + getPortOfConnectedDatabase() + "'",
                "");
        final String query = "SELECT 1 FROM VS_EXA_WITH_CONNNAME.SIMPLE_VALUES";
        final ResultSet result = executeQuery(query);
        assertNextRow(result, Short.valueOf("1"));
        matchSingleRowExplain(query,
                "IMPORT FROM EXA AT 'localhost:8888' USER 'sys' IDENTIFIED BY 'exasol' STATEMENT 'SELECT 1 FROM \"NATIVE_EXA_IT\".\"SIMPLE_VALUES\"'",
                IS_LOCAL);
    }

    @Test
    void testVirtualSchemaImportFromJDBCWithConnectionStringUserPassword() throws SQLException, FileNotFoundException {
        final String connectionString = "jdbc:exa:localhost:" + getPortOfConnectedDatabase();
        createVirtualSchema("VS_JDBC_WITH_USER_PW", ExasolSqlDialect.NAME, "", TEST_SCHEMA, "",
                getConfig().getExasolUser(), getConfig().getExasolPassword(), "ADAPTER.JDBC_ADAPTER", connectionString,
                false, "", "", null, "");
        final String query = "SELECT 1 FROM VS_JDBC_WITH_USER_PW.SIMPLE_VALUES";
        final ResultSet result = executeQuery(query);
        assertNextRow(result, Short.valueOf("1"));
        matchSingleRowExplain(query,
                "IMPORT INTO (c1 DECIMAL(1, 0)) FROM JDBC AT 'jdbc:exa:localhost:8888' USER 'sys' IDENTIFIED BY 'exasol' STATEMENT 'SELECT 1 FROM \"NATIVE_EXA_IT\".\"SIMPLE_VALUES\"'",
                IS_LOCAL);
    }

    @Test
    void testVirtualSchemaImportFromEXAWithConnectionStringUserPassword() throws SQLException, FileNotFoundException {
        final String connectionString = "jdbc:exa:localhost:" + getPortOfConnectedDatabase();
        createVirtualSchema("VS_EXA_WITH_USER_PW", ExasolSqlDialect.NAME, "", TEST_SCHEMA, "",
                getConfig().getExasolUser(), getConfig().getExasolPassword(), "ADAPTER.JDBC_ADAPTER", connectionString,
                false, "", "",
                "IMPORT_FROM_EXA = 'true' EXA_CONNECTION_STRING = 'localhost:" + getPortOfConnectedDatabase() + "'",
                "");
        final String query = "SELECT 1 FROM VS_EXA_WITH_USER_PW.SIMPLE_VALUES";
        final ResultSet result = executeQuery(query);
        assertNextRow(result, Short.valueOf("1"));
        matchSingleRowExplain(query,
                "IMPORT FROM EXA AT 'localhost:8888' USER 'sys' IDENTIFIED BY 'exasol' STATEMENT 'SELECT 1 FROM \"NATIVE_EXA_IT\".\"SIMPLE_VALUES\"'",
                IS_LOCAL);
    }

    // Join Tests -------------------------------------------------------------
    @Test
    void innerJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a INNER JOIN  %1$s.t2 b ON a.x=b.x",
                VIRTUAL_SCHEMA_JDBC);
        final ResultSet result = executeQuery(query);
        assertNextRow(result, (long) 2, "bbb", (long) 2, "bbb");
        assertFalse(result.next());
    }

    @Test
    void innerJoinWithProjection() throws SQLException {
        final String query = String.format(
                "SELECT b.y || %1$s.t1.y FROM  %1$s.t1 INNER JOIN  %1$s.t2 b ON %1$s.t1.x=b.x", VIRTUAL_SCHEMA_JDBC);
        final ResultSet result = executeQuery(query);
        assertNextRow(result, "bbbbbb");
        assertFalse(result.next());
    }

    @Test
    void leftJoin() throws SQLException {
        final String query = String.format(
                "SELECT * FROM  %1$s.t1 a LEFT OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA_JDBC);
        final ResultSet result = executeQuery(query);
        assertNextRow(result, (long) 1, "aaa", null, null);
        assertNextRow(result, (long) 2, "bbb", (long) 2, "bbb");
        assertFalse(result.next());
    }

    @Test
    void rightJoin() throws SQLException {
        final String query = String.format(
                "SELECT * FROM  %1$s.t1 a RIGHT OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA_JDBC);
        final ResultSet result = executeQuery(query);
        assertNextRow(result, (long) 2, "bbb", (long) 2, "bbb");
        assertNextRow(result, null, null, (long) 3, "ccc");
        assertFalse(result.next());
    }

    @Test
    void fullOuterJoin() throws SQLException {
        final String query = String.format(
                "SELECT * FROM  %1$s.t1 a FULL OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA_JDBC);
        final ResultSet result = executeQuery(query);
        assertNextRow(result, (long) 1, "aaa", null, null);
        assertNextRow(result, (long) 2, "bbb", (long) 2, "bbb");
        assertNextRow(result, null, null, (long) 3, "ccc");
        assertFalse(result.next());
    }
}