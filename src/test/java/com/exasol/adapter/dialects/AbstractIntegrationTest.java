package com.exasol.adapter.dialects;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractIntegrationTest {
    private static Connection connection;
    private static IntegrationTestConfig config;

    public static IntegrationTestConfig getConfig() throws FileNotFoundException {
        if (config == null) {
            config = new IntegrationTestConfig();
        }
        return config;
    }

    // You have to call this method with a connection to your EXASOL database during the @BeforeClass method of your
    // integration test
    public static void setConnection(final Connection connection) {
        AbstractIntegrationTest.connection = connection;
    }

    public static Connection getConnection() {
        return connection;
    }

    private static void checkConnection() {
        assertNotNull("Error: Your integration test did not set the connection.", connection);
    }

    public static Connection connectToExa() throws ClassNotFoundException, SQLException, FileNotFoundException {
        final String user = config.getExasolUser();
        final String password = config.getExasolPassword();
        return connectToExa(user, password);
    }

    public static Connection connectToExa(final String user, final String password)
            throws ClassNotFoundException, SQLException, FileNotFoundException {
        final String exaAddress = config.getExasolAddress();
        Class.forName("com.exasol.jdbc.EXADriver");
        return DriverManager.getConnection("jdbc:exa:" + exaAddress, user, password);
    }

    public ResultSet executeQuery(final Connection conn, final String query) throws SQLException {
        return conn.createStatement().executeQuery(query);
    }

    public ResultSet executeQuery(final String query) throws SQLException {
        checkConnection();
        return executeQuery(connection, query);
    }

    public int executeUpdate(final String query) throws SQLException {
        checkConnection();
        return connection.createStatement().executeUpdate(query);
    }

    public void execute(final String stmt) throws SQLException {
        checkConnection();
        connection.createStatement().execute(stmt);
    }

    public static void createJDBCAdapter(final Connection conn, final List<String> jarIncludes) throws SQLException {
        final Statement stmt = conn.createStatement();
        stmt.execute("CREATE SCHEMA IF NOT EXISTS ADAPTER");
        String sql = "CREATE OR REPLACE JAVA ADAPTER SCRIPT ADAPTER.JDBC_ADAPTER AS\n";
        sql += "  %scriptclass com.exasol.adapter.RequestDispatcher;\n";
        for (final String includePath : jarIncludes) {
            sql += "  %jar " + includePath + ";\n";
        }
        // sql += " %jvmoption -Xms64m -Xmx64m;";
        sql += "/";
        stmt.execute(sql);
    }

    public static void createJDBCAdapter(final List<String> jarIncludes) throws SQLException {
        checkConnection();
        createJDBCAdapter(connection, jarIncludes);
    }

    public static void createVirtualSchema(final Connection conn, final String virtualSchemaName, final String dialect,
            final String remoteCatalog, final String remoteSchema, final String connectionName, final String user,
            final String password, final String adapter, final String remoteConnectionString, final boolean isLocal,
            final String debugAddress, final String tableFilter, final String suffix, final String excludedCapabilities)
            throws SQLException {
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
        if (isLocal) {
            sql += " IS_LOCAL='" + isLocal + "'";
        }
        if (!debugAddress.isEmpty()) {
            sql += " DEBUG_ADDRESS='" + debugAddress + "'";
        }
        sql += " LOG_LEVEL='ALL'";
        if (!excludedCapabilities.isEmpty()) {
            sql += " EXCLUDED_CAPABILITIES='" + excludedCapabilities + "'";
        }
        if (!tableFilter.isEmpty()) {
            sql += " TABLE_FILTER='" + tableFilter + "'";
        }
        if (suffix != null) {
            sql += " " + suffix;
        }
        conn.createStatement().execute(sql);
    }

    public static void createVirtualSchema(final String virtualSchemaName, final String dialect,
            final String remoteCatalog, final String remoteSchema, final String connectionName, final String user,
            final String password, final String adapter, final String remoteConnectionString, final boolean isLocal,
            final String debugAddress, final String tableFilter, final String suffix, final String excludedCapabilities)
            throws SQLException {
        checkConnection();
        createVirtualSchema(connection, virtualSchemaName, dialect, remoteCatalog, remoteSchema, connectionName, user,
                password, adapter, remoteConnectionString, isLocal, debugAddress, tableFilter, suffix,
                excludedCapabilities);
    }

    public static void createConnection(final Connection conn, final String connectionName,
            final String connectionString, final String user, final String password) throws SQLException {
        removeConnection(conn, connectionName);
        String sql = "CREATE CONNECTION " + connectionName;
        sql += " TO '" + connectionString + "'";
        if (!user.equals("") && !password.equals("")) {
            sql += " USER '" + user + "'";
            sql += " IDENTIFIED BY '" + password + "'";
        }
        conn.createStatement().execute(sql);
    }

    public static void createConnection(final String connectionName, final String connectionString, final String user,
            final String password) throws SQLException {
        checkConnection();
        createConnection(connection, connectionName, connectionString, user, password);
    }

    public static String getPortOfConnectedDatabase(final Connection conn) throws SQLException {
        final ResultSet result = conn.createStatement()
                .executeQuery("SELECT PARAM_VALUE FROM EXA_COMMANDLINE where PARAM_NAME = 'port'");
        result.next();
        return result.getString("PARAM_VALUE");
    }

    public static String getPortOfConnectedDatabase() throws SQLException {
        checkConnection();
        return getPortOfConnectedDatabase(connection);
    }

    public static void assertNextRow(final ResultSet result, final Object... expectedElements) throws SQLException {
        result.next();
        assertEquals(getDiffWithTypes(Arrays.asList(expectedElements), rowToObject(result)),
                Arrays.asList(expectedElements), rowToObject(result));
    }

    public static void assertLastRow(final ResultSet result, final Object... expectedElements) throws SQLException {
        assertNextRow(result, expectedElements);
        assertFalse(result.next());
    }

    private static void removeConnection(final Connection conn, final String connectionName) throws SQLException {
        final Statement stmt = conn.createStatement();
        final String sql = "DROP CONNECTION IF EXISTS " + connectionName;
        stmt.execute(sql);
    }

    private static void removeVirtualSchema(final Connection conn, final String schemaName) throws SQLException {
        final Statement stmt = conn.createStatement();
        final String sql = "DROP VIRTUAL SCHEMA IF EXISTS " + schemaName + " CASCADE";
        stmt.execute(sql);
    }

    /**
     * This method shows the diff with the types. Normally, only the String representation is shown in the diff, so you
     * cannot distinguish between (int)1 and (long)1.
     */
    private static String getDiffWithTypes(final List<Object> expected, final List<Object> actual) {
        final StringBuilder builder = new StringBuilder();
        builder.append("expected elements <[");
        boolean first = true;
        for (final Object element : expected) {
            if (!first) {
                builder.append(", ");
            }
            if (element == null) {
                builder.append("null");
            } else {
                builder.append("(" + element.getClass().getName() + ")" + element.toString());
            }
            first = false;
        }
        builder.append("]> but was <[");
        first = true;
        for (final Object element : actual) {
            if (!first) {
                builder.append(", ");
            }
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

    public static void matchSingleRowExplain(final Connection conn, final String query, final String expectedExplain,
            final boolean isLocal) throws SQLException {
        final ResultSet result = conn.createStatement().executeQuery("EXPLAIN VIRTUAL " + query);
        result.next();
        if (isLocal) {
            assertEquals(expectedExplain, result.getString("PUSHDOWN_SQL"));
        } else {
            assertEquals(expectedExplain, extractStatementFromImport(result.getString("PUSHDOWN_SQL")));
        }
        assertEquals(false, result.next());
    }

    public static void matchSingleRowExplain(final Connection conn, final String query, final String expectedExplain)
            throws SQLException {
        checkConnection();
        matchSingleRowExplain(connection, query, expectedExplain, false);
    }

    public static void matchSingleRowExplain(final String query, final String expectedExplain, final boolean isLocal)
            throws SQLException {
        checkConnection();
        matchSingleRowExplain(connection, query, expectedExplain, isLocal);
    }

    public static void matchSingleRowExplain(final String query, final String expectedExplain) throws SQLException {
        checkConnection();
        matchSingleRowExplain(connection, query, expectedExplain);
    }

    private static List<Object> rowToObject(final ResultSet resultSet) throws SQLException {
        final int colCount = resultSet.getMetaData().getColumnCount();
        final List<Object> res = new ArrayList<>(colCount);
        for (int i = 1; i <= colCount; ++i) {
            res.add(resultSet.getObject(i));
        }
        return res;
    }

    private static String extractStatementFromImport(final String importStatement) {
        final String regexp = " STATEMENT '(.*)'";
        final Pattern pattern = Pattern.compile(regexp);
        final Matcher matcher = pattern.matcher(importStatement);
        assertTrue(matcher.find());
        final String statement = matcher.group(1);
        // Replace double single quotes, e.g. in "IMPORT ... STATEMENT 'SELECT A=''x''
        // FROM T'";
        return statement.replace("''", "'");
    }

    public Date getSqlDate(final int year, final int month, final int day) {
        // Attention: month start with 0!
        return new java.sql.Date(new GregorianCalendar(year, month - 1, day).getTime().getTime());
    }

    public Timestamp getSqlTimestamp(final int year, final int month, final int day, final int hour, final int minute,
            final int second, final int millisecond) {
        // Attention: month start with 0!
        return new java.sql.Timestamp(
                new GregorianCalendar(year, month - 1, day, hour, minute, second).getTime().getTime() + millisecond);
    }

}
