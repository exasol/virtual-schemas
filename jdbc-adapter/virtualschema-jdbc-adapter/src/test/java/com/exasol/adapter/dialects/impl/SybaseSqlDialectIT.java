package com.exasol.adapter.dialects.impl;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import org.junit.*;

import com.exasol.adapter.dialects.AbstractIntegrationTest;

public class SybaseSqlDialectIT extends AbstractIntegrationTest {
    private static final boolean IS_LOCAL = false;
    private static final String VS_NAME = "VS_SYBASE";

    @BeforeClass
    public static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().sybaseTestsRequested());

        setConnection(connectToExa());
        createSybaseJDBCAdapter();
        final String catalogName = "testdb"; // This only works for the database in our test environment
        final String schemaName = "tester";
        createVirtualSchema(VS_NAME, SybaseSqlDialect.getPublicName(), catalogName, schemaName, "",
                getConfig().getSybaseUser(), getConfig().getSybasePassword(), "ADAPTER.JDBC_ADAPTER",
                getConfig().getSybaseJdbcConnectionString(), IS_LOCAL, getConfig().debugAddress(), "", null,"");
    }

    private static void createSybaseJDBCAdapter() throws SQLException, FileNotFoundException {
        final String jdbcAdapterPath = getConfig().getJdbcAdapterPath();
        final String jdbcDriverDriver = getConfig().getSybaseJdbcDriverPath();
        final List<String> includes = new ArrayList<>();
        includes.add(jdbcAdapterPath);
        includes.add(jdbcDriverDriver);
        createJDBCAdapter(includes);
    }

    // Use getColumnTypes() to access this map
    private final Map<String, Map<String, String>> columnTypes = new HashMap<>();

    private String getColumnType(final String table, final String column) throws SQLException {
        Map<String, String> map = this.columnTypes.get(table);
        if (map == null) {
            map = getColumnTypesOfTable(table);
            this.columnTypes.put(table, map);
        }
        return map.get(column.toUpperCase());
    }

    private Map<String, String> getColumnTypesOfTable(final String table) throws SQLException {
        final Map<String, String> map = new HashMap<>();
        final ResultSet result = executeQuery("DESCRIBE " + VS_NAME + ".\"" + table + "\"");
        while (result.next()) {
            map.put(result.getString("COLUMN_NAME").toUpperCase(), result.getString("SQL_TYPE").toUpperCase());
        }
        return map;
    }

    private void assertColumnTypeEquals(final String expected, final String table, final String column)
            throws SQLException {
        assertEquals(expected.toUpperCase(), getColumnType(table, column).toUpperCase());
    }

    @Test
    public void testSelect() throws SQLException {
        final ResultSet result = executeQuery("SELECT * FROM vs_sybase.\"ittable\"");
        matchNextRow(result, "e", 2L);
    }

    @Test
    public void testProjection() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\"");
        matchNextRow(result, "e");
    }

    @Test
    public void testOrderByAsc() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\"");
        matchNextRow(result, "a");
        result.last();
        assertEquals(null, result.getObject(1));
    }

    @Test
    public void testOrderByAscNullsFirst() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" NULLS FIRST");
        result.next();
        assertEquals(null, result.getObject(1));
        result.last();
        matchLastRow(result, "z");
    }

    @Test
    public void testOrderByDesc() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" DESC");
        result.next();
        assertEquals(null, result.getObject(1));
        result.last();
        matchLastRow(result, "a");
    }

    @Test
    public void testOrderByDescNullsLast() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" DESC NULLS LAST");
        matchNextRow(result, "z");
        result.last();
        assertEquals(null, result.getObject(1));
    }

    @Test
    public void testWhereGreater() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"b\" FROM vs_sybase.\"ittable\" WHERE \"b\" > 0");
        result.last();
        assertEquals(2, result.getRow());
    }

    // TODO: add datatype tests
    @Test
    public void testTypeSmalldatetime() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_smalldatetime\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, getSqlTimestamp(1900, 1, 1, 1, 2, 0, 0));
    }

    @Test
    public void testTypeDatetime() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_datetime\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, getSqlTimestamp(1753, 1, 1, 1, 2, 3, 100));
    }

    @Test
    public void testTypeDate() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_date\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, getSqlDate(2032, 12, 3));
    }

    @Test
    public void testTypeTime() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_time\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, "11:22:33.456");
    }

    @Test
    @Ignore
    public void testTypeBigdatetime() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_bigdatetime\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, getSqlTimestamp(1753, 1, 1, 1, 2, 3, 100));
        // SQL Error [22001]: Data truncation
        // Arithmetic overflow during implicit conversion of BIGDATETIME value to a
        // DATETIME field .
    }

    @Test
    public void testTypeBigtime() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_bigtime\" FROM vs_sybase.\"timetypes\"");
        matchNextRow(result, "11:11:11.111111");
    }

    @Test
    public void testTypeBigint() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_bigint\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, new BigDecimal("-9223372036854775808"));
        matchNextRow(result, new BigDecimal("9223372036854775807"));
        assertColumnTypeEquals("DECIMAL(19,0)", "integertypes", "c_bigint");
    }

    @Test
    public void testTypeInt() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_int\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, -2147483648L);
        matchNextRow(result, 2147483647L);
        assertColumnTypeEquals("DECIMAL(10,0)", "integertypes", "c_int");
    }

    @Test
    public void testTypeSmallint() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_smallint\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, -32768);
        matchNextRow(result, 32767);
        assertColumnTypeEquals("DECIMAL(5,0)", "integertypes", "c_smallint");
    }

    @Test
    public void testTypeUbigint() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_ubigint\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, new BigDecimal("0"));
        matchNextRow(result, new BigDecimal("18446744073709551615"));
        assertColumnTypeEquals("DECIMAL(20,0)", "integertypes", "c_ubigint");
    }

    @Test
    public void testTypeUint() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_uint\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, 0L);
        matchNextRow(result, 4294967295L);
        assertColumnTypeEquals("DECIMAL(10,0)", "integertypes", "c_uint");
    }

    @Test
    public void testTypeUsmallint() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_usmallint\" FROM vs_sybase.\"integertypes\"");
        matchNextRow(result, 0);
        matchNextRow(result, 65535);
        assertColumnTypeEquals("DECIMAL(5,0)", "integertypes", "c_usmallint");
    }

    @Test
    public void testTypeNumeric36() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_numeric_36_0\" FROM vs_sybase.\"decimaltypes\"");
        matchNextRow(result, new BigDecimal("12345678901234567890123456"));
        matchNextRow(result, new BigDecimal("-12345678901234567890123456"));
        assertColumnTypeEquals("DECIMAL(36,0)", "decimaltypes", "c_numeric_36_0");
    }

    @Test
    public void testTypeNumeric38() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_numeric_38_0\" FROM vs_sybase.\"decimaltypes\"");
        matchNextRow(result, "1234567890123456789012345678");
        matchNextRow(result, "-1234567890123456789012345678");
        assertColumnTypeEquals("VARCHAR(39) UTF8", "decimaltypes", "c_numeric_38_0");
    }

    @Test
    public void testTypeDecimal2010() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_decimal_20_10\" FROM vs_sybase.\"decimaltypes\"");
        matchNextRow(result, new BigDecimal("1234567890.0123456789"));
        matchNextRow(result, new BigDecimal("-1234567890.0123456789"));
        assertColumnTypeEquals("DECIMAL(20,10)", "decimaltypes", "c_decimal_20_10");
    }

    @Test
    public void testTypeDecimal3710() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_decimal_37_10\" FROM vs_sybase.\"decimaltypes\"");
        matchNextRow(result, "12345678901234567.0123456789");
        matchNextRow(result, "-12345678901234567.0123456789");
        assertColumnTypeEquals("VARCHAR(39) UTF8", "decimaltypes", "c_decimal_37_10");
    }

    @Test
    public void testTypeDouble() throws SQLException {
        // ResultSet result = executeQuery("SELECT \"c_double\" FROM
        // vs_sybase.\"approxtypes\"");
        // matchNextRow(result, "2.2250738585072014e-308");
        // matchNextRow(result, "1.797693134862315708e+308");
        assertColumnTypeEquals("DOUBLE", "approxtypes", "c_double");
    }

    @Test
    public void testTypeReal() throws SQLException {
        // ResultSet result = executeQuery("SELECT \"c_real\" FROM
        // vs_sybase.\"approxtypes\"");
        // matchNextRow(result, new Double("1.175494351e-38"));
        // matchNextRow(result, new Double("3.402823466e+38"));
        assertColumnTypeEquals("DOUBLE", "approxtypes", "c_real");
    }

    @Test
    public void testTypeSmallmoney() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_smallmoney\" FROM vs_sybase.\"moneytypes\"");
        matchNextRow(result, new BigDecimal("214748.3647"));
        matchNextRow(result, new BigDecimal("-214748.3648"));
        assertColumnTypeEquals("DECIMAL(10,4)", "moneytypes", "c_smallmoney");
    }

    @Test
    public void testTypeMoney() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_money\" FROM vs_sybase.\"moneytypes\"");
        matchNextRow(result, new BigDecimal("922337203685477.5807"));
        matchNextRow(result, new BigDecimal("-922337203685477.5808"));
        assertColumnTypeEquals("DECIMAL(19,4)", "moneytypes", "c_money");
    }

    public String padRight(final String s, final int n) {
        return String.format("%-" + n + "s", s);
    }

    @Test
    public void testTypeChar10() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_char_10\" FROM vs_sybase.\"chartypes\"");
        matchNextRow(result, padRight("c10", 10));
        assertColumnTypeEquals("CHAR(10) ASCII", "chartypes", "c_char_10");
    }

    @Test
    public void testTypeCharTooBig() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_char_toobig\" FROM vs_sybase.\"chartypes\"");
        matchNextRow(result, padRight("c2001", 2001));
        assertColumnTypeEquals("VARCHAR(2001) ASCII", "chartypes", "c_char_toobig");
    }

    @Test
    public void testTypeVarchar() throws SQLException {
        final String column = "c_varchar";
        final String table = "chartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, "vc10");
        assertColumnTypeEquals("VARCHAR(10) ASCII", table, column);
    }

    @Test
    public void testTypeUnichar10() throws SQLException {
        final String column = "c_unichar_10";
        final String table = "chartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, padRight("uc10", 10));
        assertColumnTypeEquals("CHAR(10) UTF8", table, column);
    }

    @Test
    public void testTypeUnicharToobig() throws SQLException {
        final int fieldSize = 8148;
        final String column = "c_unichar_toobig";
        final String table = "fatunichartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, padRight("xyz", fieldSize));
        assertColumnTypeEquals("VARCHAR(" + fieldSize + ") UTF8", table, column);
    }

    @Test
    public void testTypeUnivarchar() throws SQLException {
        final String column = "c_univarchar";
        final String table = "chartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, "uvc10");
        assertColumnTypeEquals("VARCHAR(10) UTF8", table, column);
    }

    @Test
    public void testTypeNchar() throws SQLException {
        final String column = "c_nchar";
        final String table = "chartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, padRight("nc10", 10));
        assertColumnTypeEquals("CHAR(10) ASCII", table, column);
    }

    @Test
    public void testTypeNvarchar() throws SQLException {
        final String column = "c_nvarchar";
        final String table = "chartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, "nvc10");
        assertColumnTypeEquals("VARCHAR(10) ASCII", table, column);
    }

    @Test
    public void testTypeText() throws SQLException {
        final String column = "c_text";
        final String table = "texttypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, "Text. A wall of text.");
        assertColumnTypeEquals("VARCHAR(2000000) UTF8", table, column);
    }

    @Test
    public void testTypeUnitext() throws SQLException {
        final String column = "c_unitext";
        final String table = "texttypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, "Text. A wall of Unicode text.");
        assertColumnTypeEquals("VARCHAR(2000000) UTF8", table, column);
    }

    @Test
    public void testTypeBinary() throws SQLException {
        final String column = "c_binary";
        final String table = "misctypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, "binary NOT SUPPORTED");
    }

    @Test
    public void testTypeVarbinary() throws SQLException {
        final String column = "c_varbinary";
        final String table = "misctypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, "varbinary NOT SUPPORTED");
    }

    @Test
    public void testTypeImage() throws SQLException {
        final String column = "c_image";
        final String table = "misctypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, "image NOT SUPPORTED");
    }

    @Test
    public void testTypeBit() throws SQLException {
        final String column = "c_bit";
        final String table = "misctypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        matchNextRow(result, false);
        assertColumnTypeEquals("BOOLEAN", table, column);
    }
}