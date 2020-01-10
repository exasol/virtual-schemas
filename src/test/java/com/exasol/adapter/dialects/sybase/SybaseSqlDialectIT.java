package com.exasol.adapter.dialects.sybase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import org.junit.Assume;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import com.exasol.adapter.dialects.IntegrationTestConfigurationCondition;

@Tag("integration")
@ExtendWith(IntegrationTestConfigurationCondition.class)
class SybaseSqlDialectIT extends AbstractIntegrationTest {
    private static final boolean IS_LOCAL = false;
    private static final String VS_NAME = "VS_SYBASE";

    @BeforeAll
    static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().sybaseTestsRequested());

        setConnection(connectToExa());
        createSybaseJDBCAdapter();
        final String catalogName = "testdb"; // This only works for the database in our test environment
        final String schemaName = "tester";
        createVirtualSchema(VS_NAME, SybaseSqlDialect.NAME, catalogName, schemaName, "", getConfig().getSybaseUser(),
                getConfig().getSybasePassword(), "ADAPTER.JDBC_ADAPTER", getConfig().getSybaseJdbcConnectionString(),
                IS_LOCAL, getConfig().debugAddress(), "", null, "");
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
    void testSelect() throws SQLException {
        final ResultSet result = executeQuery("SELECT * FROM vs_sybase.\"ittable\"");
        assertNextRow(result, "e", 2L);
    }

    @Test
    void testProjection() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\"");
        assertNextRow(result, "e");
    }

    @Test
    void testOrderByAsc() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\"");
        assertNextRow(result, "a");
        result.last();
        assertNull(result.getObject(1));
    }

    @Test
    void testOrderByAscNullsFirst() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" NULLS FIRST");
        result.next();
        assertNull(result.getObject(1));
        result.last();
        assertLastRow(result, "z");
    }

    @Test
    void testOrderByDesc() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" DESC");
        result.next();
        assertNull(result.getObject(1));
        result.last();
        assertLastRow(result, "a");
    }

    @Test
    void testOrderByDescNullsLast() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" DESC NULLS LAST");
        assertNextRow(result, "z");
        result.last();
        assertNull(result.getObject(1));
    }

    @Test
    void testWhereGreater() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"b\" FROM vs_sybase.\"ittable\" WHERE \"b\" > 0");
        result.last();
        assertEquals(2, result.getRow());
    }

    @Test
    void testTypeSmalldatetime() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_smalldatetime\" FROM vs_sybase.\"timetypes\"");
        assertNextRow(result, getSqlTimestamp(1900, 1, 1, 1, 2, 0, 0));
    }

    @Test
    void testTypeDatetime() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_datetime\" FROM vs_sybase.\"timetypes\"");
        assertNextRow(result, getSqlTimestamp(1753, 1, 1, 1, 2, 3, 100));
    }

    @Test
    void testTypeDate() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_date\" FROM vs_sybase.\"timetypes\"");
        assertNextRow(result, getSqlDate(2032, 12, 3));
    }

    @Test
    void testTypeTime() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_time\" FROM vs_sybase.\"timetypes\"");
        assertNextRow(result, "11:22:33.456");
    }

    @Test
    void testTypeBigtime() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_bigtime\" FROM vs_sybase.\"timetypes\"");
        assertNextRow(result, "11:11:11.111111");
    }

    @Test
    void testTypeBigint() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_bigint\" FROM vs_sybase.\"integertypes\"");
        assertNextRow(result, new BigDecimal("-9223372036854775808"));
        assertNextRow(result, new BigDecimal("9223372036854775807"));
        assertColumnTypeEquals("DECIMAL(19,0)", "integertypes", "c_bigint");
    }

    @Test
    void testTypeInt() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_int\" FROM vs_sybase.\"integertypes\"");
        assertNextRow(result, -2147483648L);
        assertNextRow(result, 2147483647L);
        assertColumnTypeEquals("DECIMAL(10,0)", "integertypes", "c_int");
    }

    @Test
    void testTypeSmallint() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_smallint\" FROM vs_sybase.\"integertypes\"");
        assertNextRow(result, -32768);
        assertNextRow(result, 32767);
        assertColumnTypeEquals("DECIMAL(5,0)", "integertypes", "c_smallint");
    }

    @Test
    void testTypeUbigint() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_ubigint\" FROM vs_sybase.\"integertypes\"");
        assertNextRow(result, new BigDecimal("0"));
        assertNextRow(result, new BigDecimal("18446744073709551615"));
        assertColumnTypeEquals("DECIMAL(20,0)", "integertypes", "c_ubigint");
    }

    @Test
    void testTypeUint() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_uint\" FROM vs_sybase.\"integertypes\"");
        assertNextRow(result, 0L);
        assertNextRow(result, 4294967295L);
        assertColumnTypeEquals("DECIMAL(10,0)", "integertypes", "c_uint");
    }

    @Test
    void testTypeUsmallint() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_usmallint\" FROM vs_sybase.\"integertypes\"");
        assertNextRow(result, 0);
        assertNextRow(result, 65535);
        assertColumnTypeEquals("DECIMAL(5,0)", "integertypes", "c_usmallint");
    }

    @Test
    void testTypeNumeric36() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_numeric_36_0\" FROM vs_sybase.\"decimaltypes\"");
        assertNextRow(result, new BigDecimal("12345678901234567890123456"));
        assertNextRow(result, new BigDecimal("-12345678901234567890123456"));
        assertColumnTypeEquals("DECIMAL(36,0)", "decimaltypes", "c_numeric_36_0");
    }

    @Test
    void testTypeNumeric38() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_numeric_38_0\" FROM vs_sybase.\"decimaltypes\"");
        assertNextRow(result, "1234567890123456789012345678");
        assertNextRow(result, "-1234567890123456789012345678");
        assertColumnTypeEquals("VARCHAR(39) UTF8", "decimaltypes", "c_numeric_38_0");
    }

    @Test
    void testTypeDecimal2010() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_decimal_20_10\" FROM vs_sybase.\"decimaltypes\"");
        assertNextRow(result, new BigDecimal("1234567890.0123456789"));
        assertNextRow(result, new BigDecimal("-1234567890.0123456789"));
        assertColumnTypeEquals("DECIMAL(20,10)", "decimaltypes", "c_decimal_20_10");
    }

    @Test
    void testTypeDecimal3710() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_decimal_37_10\" FROM vs_sybase.\"decimaltypes\"");
        assertNextRow(result, "12345678901234567.0123456789");
        assertNextRow(result, "-12345678901234567.0123456789");
        assertColumnTypeEquals("VARCHAR(39) UTF8", "decimaltypes", "c_decimal_37_10");
    }

    @Test
    void testTypeDouble() throws SQLException {
        // ResultSet result = executeQuery("SELECT \"c_double\" FROM
        // vs_sybase.\"approxtypes\"");
        // matchNextRow(result, "2.2250738585072014e-308");
        // matchNextRow(result, "1.797693134862315708e+308");
        assertColumnTypeEquals("DOUBLE", "approxtypes", "c_double");
    }

    @Test
    void testTypeReal() throws SQLException {
        // ResultSet result = executeQuery("SELECT \"c_real\" FROM
        // vs_sybase.\"approxtypes\"");
        // matchNextRow(result, new Double("1.175494351e-38"));
        // matchNextRow(result, new Double("3.402823466e+38"));
        assertColumnTypeEquals("DOUBLE", "approxtypes", "c_real");
    }

    @Test
    void testTypeSmallmoney() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_smallmoney\" FROM vs_sybase.\"moneytypes\"");
        assertNextRow(result, new BigDecimal("214748.3647"));
        assertNextRow(result, new BigDecimal("-214748.3648"));
        assertColumnTypeEquals("DECIMAL(10,4)", "moneytypes", "c_smallmoney");
    }

    @Test
    void testTypeMoney() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_money\" FROM vs_sybase.\"moneytypes\"");
        assertNextRow(result, new BigDecimal("922337203685477.5807"));
        assertNextRow(result, new BigDecimal("-922337203685477.5808"));
        assertColumnTypeEquals("DECIMAL(19,4)", "moneytypes", "c_money");
    }

    String padRight(final String s, final int n) {
        return String.format("%-" + n + "s", s);
    }

    @Test
    void testTypeChar10() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_char_10\" FROM vs_sybase.\"chartypes\"");
        assertNextRow(result, padRight("c10", 10));
        assertColumnTypeEquals("CHAR(10) ASCII", "chartypes", "c_char_10");
    }

    @Test
    void testTypeCharTooBig() throws SQLException {
        final ResultSet result = executeQuery("SELECT \"c_char_toobig\" FROM vs_sybase.\"chartypes\"");
        assertNextRow(result, padRight("c2001", 2001));
        assertColumnTypeEquals("VARCHAR(2001) ASCII", "chartypes", "c_char_toobig");
    }

    @Test
    void testTypeVarchar() throws SQLException {
        final String column = "c_varchar";
        final String table = "chartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, "vc10");
        assertColumnTypeEquals("VARCHAR(10) ASCII", table, column);
    }

    @Test
    void testTypeUnichar10() throws SQLException {
        final String column = "c_unichar_10";
        final String table = "chartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, padRight("uc10", 10));
        assertColumnTypeEquals("CHAR(10) UTF8", table, column);
    }

    @Test
    void testTypeUnicharToobig() throws SQLException {
        final int fieldSize = 8148;
        final String column = "c_unichar_toobig";
        final String table = "fatunichartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, padRight("xyz", fieldSize));
        assertColumnTypeEquals("VARCHAR(" + fieldSize + ") UTF8", table, column);
    }

    @Test
    void testTypeUnivarchar() throws SQLException {
        final String column = "c_univarchar";
        final String table = "chartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, "uvc10");
        assertColumnTypeEquals("VARCHAR(10) UTF8", table, column);
    }

    @Test
    void testTypeNchar() throws SQLException {
        final String column = "c_nchar";
        final String table = "chartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, padRight("nc10", 10));
        assertColumnTypeEquals("CHAR(10) ASCII", table, column);
    }

    @Test
    void testTypeNvarchar() throws SQLException {
        final String column = "c_nvarchar";
        final String table = "chartypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, "nvc10");
        assertColumnTypeEquals("VARCHAR(10) ASCII", table, column);
    }

    @Test
    void testTypeText() throws SQLException {
        final String column = "c_text";
        final String table = "texttypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, "Text. A wall of text.");
        assertColumnTypeEquals("VARCHAR(2000000) UTF8", table, column);
    }

    @Test
    void testTypeUnitext() throws SQLException {
        final String column = "c_unitext";
        final String table = "texttypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, "Text. A wall of Unicode text.");
        assertColumnTypeEquals("VARCHAR(2000000) UTF8", table, column);
    }

    @Test
    void testTypeBinary() throws SQLException {
        final String column = "c_binary";
        final String table = "misctypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, "binary NOT SUPPORTED");
    }

    @Test
    void testTypeVarbinary() throws SQLException {
        final String column = "c_varbinary";
        final String table = "misctypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, "varbinary NOT SUPPORTED");
    }

    @Test
    void testTypeImage() throws SQLException {
        final String column = "c_image";
        final String table = "misctypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, "image NOT SUPPORTED");
    }

    @Test
    void testTypeBit() throws SQLException {
        final String column = "c_bit";
        final String table = "misctypes";
        final ResultSet result = executeQuery("SELECT \"" + column + "\" FROM vs_sybase.\"" + table + "\"");
        assertNextRow(result, false);
        assertColumnTypeEquals("BOOLEAN", table, column);
    }
}