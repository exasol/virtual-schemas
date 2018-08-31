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
import java.util.Arrays;
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

    // TODO: add datatype tests
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


    @Test
    public void testTypeNumeric36() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_numeric_36_0\" FROM vs_sybase.\"decimaltypes\"");
        matchNextRow(result, new BigDecimal("12345678901234567890123456"));
        matchNextRow(result, new BigDecimal("-12345678901234567890123456"));
        assertColumnTypeEquals("DECIMAL(36,0)", "decimaltypes", "c_numeric_36_0");
    }

    @Test
    public void testTypeNumeric38() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_numeric_38_0\" FROM vs_sybase.\"decimaltypes\"");
        matchNextRow(result, "1234567890123456789012345678");
        matchNextRow(result, "-1234567890123456789012345678");
        assertColumnTypeEquals("VARCHAR(39) UTF8", "decimaltypes", "c_numeric_38_0");
    }

    @Test
    public void testTypeDecimal2010() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_decimal_20_10\" FROM vs_sybase.\"decimaltypes\"");
        matchNextRow(result, new BigDecimal("1234567890.0123456789"));
        matchNextRow(result, new BigDecimal("-1234567890.0123456789"));
        assertColumnTypeEquals("DECIMAL(20,10)", "decimaltypes", "c_decimal_20_10");
    }

    @Test
    public void testTypeDecimal3710() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_decimal_37_10\" FROM vs_sybase.\"decimaltypes\"");
        matchNextRow(result, "12345678901234567.0123456789");
        matchNextRow(result, "-12345678901234567.0123456789");
        assertColumnTypeEquals("VARCHAR(39) UTF8", "decimaltypes", "c_decimal_37_10");
    }


    @Test
    public void testTypeDouble() throws SQLException {
        // ResultSet result = executeQuery("SELECT \"c_double\" FROM vs_sybase.\"approxtypes\"");
        // matchNextRow(result, "2.2250738585072014e-308");
        // matchNextRow(result, "1.797693134862315708e+308");
        assertColumnTypeEquals("DOUBLE", "approxtypes", "c_double");
    }

    @Test
    public void testTypeReal() throws SQLException {
        // ResultSet result = executeQuery("SELECT \"c_real\" FROM vs_sybase.\"approxtypes\"");
        // matchNextRow(result, new Double("1.175494351e-38"));
        // matchNextRow(result, new Double("3.402823466e+38"));
        assertColumnTypeEquals("DOUBLE", "approxtypes", "c_real");
    }


    @Test
    public void testTypeSmallmoney() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_smallmoney\" FROM vs_sybase.\"moneytypes\"");
        matchNextRow(result, new BigDecimal("214748.3647"));
        matchNextRow(result, new BigDecimal("-214748.3648"));
        assertColumnTypeEquals("DECIMAL(10,4)", "moneytypes", "c_smallmoney");
    }

    @Test
    public void testTypeMoney() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_money\" FROM vs_sybase.\"moneytypes\"");
        matchNextRow(result, new BigDecimal("922337203685477.5807"));
        matchNextRow(result, new BigDecimal("-922337203685477.5808"));
        assertColumnTypeEquals("DECIMAL(19,4)", "moneytypes", "c_money");
    }


    public String padRight(String s, int n) {
        return String.format("%-"+n+"s", s);
    }

    @Test
    public void testTypeChar10() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_char_10\" FROM vs_sybase.\"chartypes\"");
        matchNextRow(result, padRight("abcd", 10));
        assertColumnTypeEquals("CHAR(10) ASCII", "chartypes", "c_char_10");
    }

    @Test
    public void testTypeCharTooBig() throws SQLException {
        ResultSet result = executeQuery("SELECT \"c_char_toobig\" FROM vs_sybase.\"chartypes\"");
        matchNextRow(result, padRight("Lorem ipsum dolor sit amet... rest is zero.", 2001));
        assertColumnTypeEquals("VARCHAR(2001) ASCII", "chartypes", "c_char_toobig");
    }

    @Test
    public void testTypeVarchar() throws SQLException {
        String column = "c_varchar";
        String table = "chartypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, "Lorem.");
        assertColumnTypeEquals("VARCHAR(10) ASCII", table, column);
    }

    @Test
    public void testTypeUnichar10() throws SQLException {
        String column = "c_unichar_10";
        String table = "chartypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, "Ipsum.");
        assertColumnTypeEquals("CHAR(10) UTF8", table, column);
    }

    @Test
    public void testTypeUnicharToobig() throws SQLException {
        String column = "c_unichar_toobig";
        String table = "chartypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, "xyz");
        assertColumnTypeEquals("VARCHAR(8192) UTF8", table, column);
    }

    @Test
    public void testTypeUnivarchar() throws SQLException {
        String column = "c_univarchar";
        String table = "chartypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, "Dolor.");
        assertColumnTypeEquals("VARCHAR(10) UTF8", table, column);
    }

    @Test
    public void testTypeNchar() throws SQLException {
        String column = "c_nchar";
        String table = "chartypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, "Sit.");
        assertColumnTypeEquals("CHAR(10) ASCII", table, column);
    }

    @Test
    public void testTypeNvarchar() throws SQLException {
        String column = "c_nvarchar";
        String table = "chartypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, "Amet.");
        assertColumnTypeEquals("VARCHAR(10) ASCII", table, column);
    }

    @Test
    public void testTypeText() throws SQLException {
        String column = "c_text";
        String table = "chartypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, "Text. A wall of text.");
        assertColumnTypeEquals("VARCHAR(2000000) UTF8", table, column);
    }

    @Test
    public void testTypeUnitext() throws SQLException {
        String column = "c_unitext";
        String table = "chartypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, "Text. A wall of Unicode text.");
        assertColumnTypeEquals("VARCHAR(2000000) UTF8", table, column);
    }

    private static final byte[] DEADBEEF = {0xd, 0xe, 0xa, 0xd, 0xb, 0xe, 0xe, 0xf};

    @Test
    public void testTypeBinary() throws SQLException {
        String column = "c_binary";
        String table = "misctypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, "binary NOT SUPPORTED");
    }

    @Test
    public void testTypeVarbinary() throws SQLException {
        String column = "c_varbinary";
        String table = "misctypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, "varbinary NOT SUPPORTED");
    }

    @Test
    public void testTypeImage() throws SQLException {
        String column = "c_image";
        String table = "misctypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, "image NOT SUPPORTED");
    }

    @Test
    public void testTypeBit() throws SQLException {
        String column = "c_bit";
        String table = "misctypes";
        ResultSet result = executeQuery("SELECT \""+column+"\" FROM vs_sybase.\""+table+"\"");
        matchNextRow(result, false);
        assertColumnTypeEquals("BOOLEAN", table, column);
    }

}
