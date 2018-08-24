package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;


import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class SybaseSqlDialectIT extends AbstractIntegrationTest {

    private static final boolean IS_LOCAL = false;

    private static final String VS_NAME = "VS_SYBASE";

    @BeforeClass
    public static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().sybaseTestsRequested());

        setConnection(connectToExa());
        createSybaseJDBCAdapter();
        String catalogName = "testdb";       // This only works for the database in our test environment
        String schemaName = "tester";
        createVirtualSchema(VS_NAME,
                SybaseSqlDialect.NAME,
                catalogName,
                schemaName,
                "",
                getConfig().getSybaseUser(),
                getConfig().getSybasePassword(),
                "ADAPTER.JDBC_ADAPTER",
                getConfig().getSybaseJdbcConnectionString(),
                IS_LOCAL,
                getConfig().debugAddress(),
                "", null);
    }

    private static void createSybaseJDBCAdapter() throws SQLException, FileNotFoundException {
        String jdbcAdapterPath = getConfig().getJdbcAdapterPath();
        String jdbcDriverDriver = getConfig().getSybaseJdbcDriverPath();
        List<String> includes = new ArrayList<>();
        includes.add(jdbcAdapterPath);
        includes.add(jdbcDriverDriver);
        createJDBCAdapter(includes);
    }

    // Use getColumnTypes() to access this map
    private Map<String, Map<String, String>> columnTypes = new HashMap<>();

    private String getColumnType(String table, String column) throws SQLException {
        Map<String, String> map = columnTypes.get(table);
        if (map == null) {
            map = getColumnTypesOfTable(table);
            columnTypes.put(table, map);
        }
        return map.get(column.toUpperCase());
    }

    private Map<String, String> getColumnTypesOfTable(String table) throws SQLException {
        Map<String, String> map = new HashMap<>();
        ResultSet result = executeQuery("DESCRIBE " + VS_NAME + ".\"" + table +"\"");
        while (result.next()) {
            map.put(result.getString("COLUMN_NAME").toUpperCase(), result.getString("SQL_TYPE").toUpperCase());
        }
        return map;
    }

    private void assertColumnTypeEquals(String expected, String table, String column) throws SQLException {
        assertEquals(expected.toUpperCase(), getColumnType(table, column).toUpperCase());
    }

    @Test
    public void testSelect() throws SQLException {
        ResultSet result = executeQuery("SELECT * FROM vs_sybase.\"ittable\"");
        matchNextRow(result, "e", 2L);
    }

    @Test
    public void testProjection() throws SQLException {
        ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\"");
        matchNextRow(result, "e");
    }

    @Test
    public void testOrderByAsc() throws SQLException {
        ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\"");
        matchNextRow(result, "a");
        result.last();
        assertEquals(null, result.getObject(1));
    }

    @Test
    public void testOrderByAscNullsFirst() throws SQLException {
        ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" NULLS FIRST");
        result.next();
        assertEquals(null, result.getObject(1));
        result.last();
        matchLastRow(result, "z");
    }

    @Test
    public void testOrderByDesc() throws SQLException {
        ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" DESC");
        result.next();
        assertEquals(null, result.getObject(1));
        result.last();
        matchLastRow(result, "a");
    }

    @Test
    public void testOrderByDescNullsLast() throws SQLException {
        ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" DESC NULLS LAST");
        matchNextRow(result, "z");
        result.last();
        assertEquals(null, result.getObject(1));
    }

    @Test
    public void testWhereGreater() throws SQLException {
        ResultSet result = executeQuery("SELECT \"b\" FROM vs_sybase.\"ittable\" WHERE \"b\" > 0");
        result.last();
        assertEquals(2, result.getRow());
    }

    @Test
    public void testTypeSmalldatetime() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_smalldatetime\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, getSqlTimestamp(1900, 1, 1, 1, 2, 0, 0));
    }

    @Test
    public void testTypeDatetime() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_datetime\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, getSqlTimestamp(1753, 1, 1, 1, 2, 3, 100));
    }

    @Test
    public void testTypeDate() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_date\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, getSqlDate(2032, 12, 3));
    }

    @Test
    public void testTypeTime() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_time\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, "11:22:33.456");
    }

    @Test
    @Ignore
    public void testTypeBigdatetime() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_bigdatetime\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, getSqlTimestamp(1753, 1, 1, 1, 2, 3, 100));
        // SQL Error [22001]: Data truncation
        //   Arithmetic overflow during implicit conversion of BIGDATETIME value to a DATETIME field .
    }

    @Test
    public void testTypeBigtime() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_bigtime\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, "11:11:11.111111");
    }


    @Test
    public void testTypeBigint() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_bigint\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, new BigDecimal("-9223372036854775808"));
        matchNextRow(result, new BigDecimal("9223372036854775807"));
        assertColumnTypeEquals("DECIMAL(19,0)", "integertypes", "c_bigint");
    }

    @Test
    public void testTypeInt() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_int\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, -2147483648L);
        matchNextRow(result, 2147483647L);
        assertColumnTypeEquals("DECIMAL(10,0)", "integertypes", "c_int");
    }

    @Test
    public void testTypeSmallint() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_smallint\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, -32768);
        matchNextRow(result, 32767);
        assertColumnTypeEquals("DECIMAL(5,0)", "integertypes", "c_smallint");
    }

    @Test
    public void testTypeUbigint() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_ubigint\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, new BigDecimal("0"));
        matchNextRow(result, new BigDecimal("18446744073709551615"));
        assertColumnTypeEquals("DECIMAL(20,0)", "integertypes", "c_ubigint");
    }

    @Test
    public void testTypeUint() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_uint\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, 0L);
        matchNextRow(result, 4294967295L);
        assertColumnTypeEquals("DECIMAL(10,0)", "integertypes", "c_uint");
    }

    @Test
    public void testTypeUsmallint() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_usmallint\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, 0);
        matchNextRow(result, 65535);
        assertColumnTypeEquals("DECIMAL(5,0)", "integertypes", "c_usmallint");
    }
}
