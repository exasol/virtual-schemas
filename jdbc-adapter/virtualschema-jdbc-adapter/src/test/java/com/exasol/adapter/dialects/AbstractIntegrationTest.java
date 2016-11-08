package com.exasol.adapter.dialects;

import java.io.FileNotFoundException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class AbstractIntegrationTest {

    private static Connection connection;

    private static IntegrationTestConfig config;

    public static IntegrationTestConfig getConfig() throws FileNotFoundException {
        if (config == null) {
            config = new IntegrationTestConfig();
        }
        return config;
    }

    /**
     * You have to call this method with a connection to your EXASOL database during the @BeforeClass method of your integration test
     */
    public static void setConnection(Connection connection) {
        AbstractIntegrationTest.connection = connection;
    }

    public static Connection getConnection() {
        return connection;
    }

    private static void checkConnection() {
        assertNotNull("Error: Your integration test did not set the connection.", connection);
    }

    public static Connection connectToExa() throws ClassNotFoundException, SQLException, FileNotFoundException {
        String user = config.getExasolUser();
        String password = config.getExasolPassword();
        return connectToExa(user, password);
    }

    public static Connection connectToExa(String user, String password) throws ClassNotFoundException, SQLException, FileNotFoundException {
        String exaAddress = config.getExasolAddress();
        Class.forName("com.exasol.jdbc.EXADriver");
        return DriverManager.getConnection("jdbc:exa:" + exaAddress, user, password);
    }

    public ResultSet executeQuery(Connection conn, String query) throws SQLException {
        return conn.createStatement().executeQuery(query);
    }

    public ResultSet executeQuery(String query) throws SQLException {
        checkConnection();
        return executeQuery(connection, query);
    }

    public static void createJDBCAdapter(Connection conn, List<String> jarIncludes) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE SCHEMA IF NOT EXISTS ADAPTER");
        String sql = "CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS\n";
        sql += "%scriptclass com.exasol.adapter.jdbc.JdbcAdapter;";
        for (String includePath : jarIncludes) {
            sql += " %jar " + includePath + ";\n";
        }
        //sql += " %jvmoption -Xms64m -Xmx64m;";
        sql += "/";
        stmt.execute(sql);
    }

    public static void createJDBCAdapter(List<String> jarIncludes) throws SQLException {
        checkConnection();
        createJDBCAdapter(connection, jarIncludes);
    }

    public static void createVirtualSchema(Connection conn, String virtualSchemaName, String dialect, String remoteCatalog, String remoteSchema, String connectionName, String user, String password, String adapter, String remoteConnectionString, boolean isLocal, String debugAddress, String tableFilter) throws SQLException {
        removeVirtualSchema(conn, virtualSchemaName);
        String sql = "CREATE VIRTUAL SCHEMA " + virtualSchemaName;
        sql += " USING " + adapter;
        sql += " WITH";
        if (!connectionName.isEmpty()) {
            assertEquals("", remoteConnectionString);
            assertEquals("", user);
            assertEquals("", password);
            sql += " CONNECTION_NAME='" + connectionName + "'";
        }
        if (!remoteConnectionString.isEmpty()) {
            sql += " CONNECTION_STRING='" + remoteConnectionString + "'";
        }
        if (!user.isEmpty()) {
            sql += " USERNAME='" + user + "'";
        }
        if (!password.isEmpty()) {
            sql += " PASSWORD='" + password + "'";
        }
        if (!remoteCatalog.isEmpty()) {
            sql += " CATALOG_NAME='" + remoteCatalog + "'";
        }
        if (!remoteSchema.isEmpty()) {
            sql += " SCHEMA_NAME='" + remoteSchema + "'";
        }
        sql += " SQL_DIALECT='" + dialect + "'";
        sql += " IS_LOCAL='" + isLocal + "'";
        if (!debugAddress.isEmpty()) {
            sql += " DEBUG_ADDRESS='" + debugAddress + "'";
        }
        if (!tableFilter.isEmpty()) {
            sql += " TABLE_FILTER='" + tableFilter + "'";
        }
        conn.createStatement().execute(sql);
    }

    public static void createVirtualSchema(String virtualSchemaName, String dialect, String remoteCatalog, String remoteSchema, String connectionName, String user, String password, String adapter, String remoteConnectionString, boolean isLocal, String debugAddress, String tableFilter) throws SQLException {
        checkConnection();
        createVirtualSchema(connection, virtualSchemaName, dialect, remoteCatalog, remoteSchema, connectionName, user, password, adapter, remoteConnectionString, isLocal, debugAddress, tableFilter);
    }

    public static void createConnection(Connection conn, String connectionName, String connectionString, String user, String password) throws SQLException {
        removeConnection(conn, connectionName);
        String sql = "CREATE CONNECTION " + connectionName;
        sql += " TO '" + connectionString + "'";
        sql += " USER '" + user + "'";
        sql += " IDENTIFIED BY '" + password +"'";
        conn.createStatement().execute(sql);
    }

    public static void createConnection(String connectionName, String connectionString, String user, String password) throws SQLException {
        checkConnection();
        createConnection(connection, connectionName, connectionString, user, password);
    }

    public static String getPortOfConnectedDatabase(Connection conn) throws SQLException {
        ResultSet result = conn.createStatement().executeQuery("SELECT PARAM_VALUE FROM EXA_COMMANDLINE where PARAM_NAME = 'port'");
        result.next();
        return result.getString("PARAM_VALUE");
    }

    public static String getPortOfConnectedDatabase() throws SQLException {
        checkConnection();
        return getPortOfConnectedDatabase(connection);
    }

    public static void matchNextRow(ResultSet result, Object... expectedElements) throws SQLException {
        result.next();
        assertEquals(getDiffWithTypes(Arrays.asList(expectedElements), rowToObject(result)), Arrays.asList(expectedElements), rowToObject(result));
    }

    public static void matchLastRow(ResultSet result, Object... expectedElements) throws SQLException {
        matchNextRow(result, expectedElements);
        assertFalse(result.next());
    }

    private static void removeConnection(Connection conn, String connectionName) throws SQLException {
        Statement stmt = conn.createStatement();
        String sql = "DROP CONNECTION IF EXISTS " + connectionName;
        stmt.execute(sql);
    }

    private static void removeVirtualSchema(Connection conn, String schemaName) throws SQLException {
        Statement stmt = conn.createStatement();
        String sql = "DROP VIRTUAL SCHEMA IF EXISTS " + schemaName + " CASCADE";
        stmt.execute(sql);
    }

    /**
     * This method shows the diff with the types. Normally, only the String representation is shown in the diff, so you cannot distinguish between (int)1 and (long)1.
     */
    private static String getDiffWithTypes(List<Object> expected, List<Object> actual) {
        StringBuilder builder = new StringBuilder();
        builder.append("expected elements <[");
        boolean first = true;
        for (Object element : expected) {
            if (!first) { builder.append(", "); }
            if (element == null) {
                builder.append("null");
            } else {
                builder.append("(" + element.getClass().getName() + ")" + element.toString());
            }
            first = false;
        }
        builder.append("]> but was <[");
        first = true;
        for (Object element : actual) {
            if (!first) { builder.append(", "); }
            if (element == null) {
                builder.append("null");
            } else {
                builder.append("(" + element.getClass().getName() + ")" + element.toString());
            }
            first = false;
        }
        builder.append("]>\n");
        return builder.toString();
    }

    public static void matchSingleRowExplain(Connection conn, String query, String expectedExplain, boolean isLocal) throws SQLException {
        ResultSet result = conn.createStatement().executeQuery("EXPLAIN VIRTUAL " + query);
        result.next();
        if (isLocal) {
            assertEquals(expectedExplain, result.getString("PUSHDOWN_SQL"));
        } else {
            assertEquals(expectedExplain, extractStatementFromImport(result.getString("PUSHDOWN_SQL")));
        }
        assertEquals(false, result.next());
    }

    public static void matchSingleRowExplain(Connection conn, String query, String expectedExplain) throws SQLException {
        checkConnection();
        matchSingleRowExplain(connection, query, expectedExplain, false);
    }

    public static void matchSingleRowExplain(String query, String expectedExplain, boolean isLocal) throws SQLException {
        checkConnection();
        matchSingleRowExplain(connection, query, expectedExplain, isLocal);
    }

    public static void matchSingleRowExplain(String query, String expectedExplain) throws SQLException {
        checkConnection();
        matchSingleRowExplain(connection, query, expectedExplain);
    }

    private static List<Object> rowToObject(ResultSet resultSet) throws SQLException {
        int colCount = resultSet.getMetaData().getColumnCount();
        List<Object> res = new ArrayList<>(colCount);
        for (int i=1; i<=colCount; ++i) {
            String type = (resultSet.getObject(i) == null) ? "null" : resultSet.getObject(i).getClass().getName();
            // System.out.println("- col " + i + " type: " + type);
            res.add(resultSet.getObject(i));
        }
        return res;
    }

    private static String extractStatementFromImport(String importStatement) {
        String regexp = " STATEMENT '(.*)'";
        Pattern pattern = Pattern.compile(regexp);
        Matcher matcher = pattern.matcher(importStatement);
        assertTrue(matcher.find());
        String statement = matcher.group(1);
        // Replace double single quotes, e.g. in "IMPORT ... STATEMENT 'SELECT A=''x'' FROM T'";
        return statement.replace("''", "'");
    }

    public Date getSqlDate(int year, int month, int day) {
        // Attention: month start with 0!
        return new java.sql.Date(new GregorianCalendar(year, month-1, day).getTime().getTime());
    }

    public Timestamp getSqlTimestamp(int year, int month, int day, int hour, int minute, int second, int millisecond) {
        // Attention: month start with 0!
        return new java.sql.Timestamp(new GregorianCalendar(year, month-1, day, hour, minute, second).getTime().getTime() + millisecond);
    }

}
