package com.exasol.adapter.dialects.impl;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.exasol.adapter.dialects.AbstractIntegrationTest;

/**
 * Integration test for the Hive SQL dialect
 *
 * Testdata: ALL_HIVE_DATA_TYPES
 */
public class HiveSqlDialectIT extends AbstractIntegrationTest {

    private static final String VIRTUAL_SCHEMA = "VS_HIVE";
    private static final String HIVE_SCHEMA = "xperience";
    private static final boolean IS_LOCAL = false;

    @BeforeClass
    public static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().hiveTestsRequested());
        setConnection(connectToExa());

        createHiveJDBCAdapter();
        createVirtualSchema(VIRTUAL_SCHEMA, HiveSqlDialect.getPublicName(), "", HIVE_SCHEMA, "", "hdfs", "hdfs",
                "ADAPTER.JDBC_ADAPTER", getConfig().getHiveJdbcConnectionString(), IS_LOCAL, getConfig().debugAddress(),
                "ALL_HIVE_DATA_TYPES", null,"");
    }

    @Test
    public void testTypeMapping() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final ResultSet result = executeQuery(
                "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, COLUMN_DEFAULT FROM EXA_DBA_COLUMNS WHERE COLUMN_SCHEMA = '"
                        + VIRTUAL_SCHEMA + "' AND COLUMN_TABLE='ALL_HIVE_DATA_TYPES' ORDER BY COLUMN_ORDINAL_POSITION");
        matchNextRow(result, "ARRAYCOL", "VARCHAR(255) ASCII", (long) 255, null, null, null);
        matchNextRow(result, "BIGINTEGER", "DECIMAL(19,0)", (long) 19, (long) 19, (long) 0, null);
        matchNextRow(result, "BOOLCOLUMN", "BOOLEAN", (long) 1, null, null, null);
        matchNextRow(result, "CHARCOLUMN", "CHAR(1) UTF8", (long) 1, null, null, null);
        matchNextRow(result, "DECIMALCOL", "DECIMAL(10,0)", (long) 10, (long) 10, (long) 0, null);
        matchNextRow(result, "DOUBLECOL", "DOUBLE", (long) 64, null, null, null);
        matchNextRow(result, "FLOATCOL", "DOUBLE", (long) 64, null, null, null);
        matchNextRow(result, "INTCOL", "DECIMAL(10,0)", (long) 10, (long) 10, (long) 0, null);
        matchNextRow(result, "MAPCOL", "VARCHAR(255) ASCII", (long) 255, null, null, null);
        matchNextRow(result, "SMALLINTEGER", "DECIMAL(5,0)", (long) 5, (long) 5, (long) 0, null);
        matchNextRow(result, "STRINGCOL", "VARCHAR(255) ASCII", (long) 255, null, null, null);
        matchNextRow(result, "STRUCTCOL", "VARCHAR(255) ASCII", (long) 255, null, null, null);
        matchNextRow(result, "TIMESTAMPCOL", "TIMESTAMP", (long) 29, null, null, null);
        matchNextRow(result, "TINYINTEGER", "DECIMAL(3,0)", (long) 3, (long) 3, (long) 0, null);
        matchNextRow(result, "VARCHARCOL", "VARCHAR(10) UTF8", (long) 10, null, null, null);
        matchNextRow(result, "BINARYCOL", "VARCHAR(2000000) UTF8", (long) 2000000, null, null, null);
        matchLastRow(result, "DATECOL", "DATE", (long) 10, null, null, null);
    }

    @Test
    public void testSelectWithAllTypes() throws SQLException {
        final ResultSet result = executeQuery("SELECT * from " + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES");
        matchNextRow(result, "[\"etet\",\"ettee\"]", new BigDecimal("56"), true, "2", (long) 53, 56.3,
                5.199999809265137, (long) 85, "{\"jkljj\":5}", 2, "tshg", "{\"a\":\"value\",\"b\":{\"c\":8}}",
                getSqlTimestamp(2017, 1, 2, 13, 32, 50, 744), (short) 1, "tytu", "MTAxMA==", getSqlDate(1970, 1, 1));
    }

    @Test
    public void testProjection() throws SQLException {
        final String query = "SELECT BIGINTEGER FROM " + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("56"));
        matchSingleRowExplain(query, "SELECT `BIGINTEGER` FROM `xperience`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    public void testRewrittenProjection() throws SQLException {
        final String query = "SELECT BINARYCOL FROM " + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "MTAxMA==");
        matchSingleRowExplain(query, "SELECT base64(`BINARYCOL`) FROM `xperience`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    public void testAggregateGroupByColumn() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String query = "SELECT boolcolumn, min(biginteger) FROM " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES GROUP BY boolcolumn";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, false, new BigDecimal("56"));
        matchNextRow(result, true, new BigDecimal("51"));
        matchSingleRowExplain(query,
                "SELECT `BOOLCOLUMN`, MIN(`BIGINTEGER`) FROM `xperience`.`ALL_HIVE_DATA_TYPES` GROUP BY `BOOLCOLUMN`");
    }

    @Test
    public void testAggregateHaving() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String query = "SELECT boolcolumn, min(biginteger) FROM " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES GROUP BY boolcolumn having min(biginteger)<56";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, true, new BigDecimal("51"));
        matchSingleRowExplain(query,
                "SELECT `BOOLCOLUMN`, MIN(`BIGINTEGER`) FROM `xperience`.`ALL_HIVE_DATA_TYPES` GROUP BY `BOOLCOLUMN` HAVING MIN(`BIGINTEGER`) < 56");
    }

    @Test
    public void testComparisonPredicates() throws SQLException {
        // =, !=, <, <=, >, >=
        final String query = "select biginteger, biginteger=60, biginteger!=60, biginteger<60, biginteger<=60, biginteger>60, biginteger>=60 from "
                + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES where intcol = 85";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("56"), false, true, true, true, false, false);
        matchSingleRowExplain(query,
                "SELECT `BIGINTEGER`, `BIGINTEGER` = 60, `BIGINTEGER` != 60, `BIGINTEGER` < 60, `BIGINTEGER` <= 60, 60 < `BIGINTEGER`,"
                        + " 60 <= `BIGINTEGER` FROM `xperience`.`ALL_HIVE_DATA_TYPES` WHERE `INTCOL` = 85");
    }

    @Test
    public void testLogicalPredicates() throws SQLException {
        // NOT, AND, OR
        final String query = "select biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES where (biginteger < 56 or biginteger > 56) and not (biginteger is null)";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("51"));
        matchNextRow(result, new BigDecimal("60"));
        matchSingleRowExplain(query, "SELECT `BIGINTEGER` FROM `xperience`.`ALL_HIVE_DATA_TYPES` "
                + "WHERE ((`BIGINTEGER` < 56 OR 56 < `BIGINTEGER`) AND NOT (`BIGINTEGER` IS NULL))");
    }

    @Test
    public void testLikePredicates() throws SQLException {
        // LIKE, LIKE ESCAPE (not pushed down)
        final String query = "select varcharcol, varcharcol like 't%' escape 't' from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES where (varcharcol like 't%')";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "tytu", false);
        matchSingleRowExplain(query,
                "SELECT `VARCHARCOL` FROM `xperience`.`ALL_HIVE_DATA_TYPES` WHERE `VARCHARCOL` LIKE 't%'");
    }

    @Test
    public void testLikePredicatesRewritten() throws SQLException {
        // REGEXP_LIKE rewritten to REGEXP
        final String query = "select varcharcol from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES where varcharcol REGEXP_LIKE 'a+'";
        final ResultSet result = executeQuery(query);
        matchLastRow(result, "anotherStr");
        matchSingleRowExplain(query,
                "SELECT `VARCHARCOL` FROM `xperience`.`ALL_HIVE_DATA_TYPES` WHERE `VARCHARCOL`REGEXP'a+'");
    }

    @Test
    public void testMiscPredicates() throws SQLException {
        // BETWEEN, IN, IS NULL, !=NULL(rewritten to "IS NOT NULL")
        final String query = "select biginteger, biginteger in (56, 61), biginteger is null, biginteger != null from "
                + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES WHERE biginteger between 51 and 60";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("56"), true, false, true);
        matchNextRow(result, new BigDecimal("51"), false, false, true);
        matchSingleRowExplain(query, "SELECT `BIGINTEGER`, `BIGINTEGER` IN (56, 61), `BIGINTEGER` IS NULL, "
                + "`BIGINTEGER` IS NOT NULL FROM `xperience`.`ALL_HIVE_DATA_TYPES` WHERE `BIGINTEGER` BETWEEN 51 AND 60");
    }

    @Test
    public void testCountSumAggregateFunction() throws SQLException {
        final String query = "SELECT COUNT(biginteger), COUNT(*), COUNT(DISTINCT biginteger), SUM(biginteger), SUM(DISTINCT biginteger) from "
                + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("7"), new BigDecimal("8"), new BigDecimal("3"), 403.0, 167.0);
        matchSingleRowExplain(query,
                "SELECT COUNT(`BIGINTEGER`), COUNT(*), COUNT(DISTINCT `BIGINTEGER`), SUM(`BIGINTEGER`), SUM(DISTINCT `BIGINTEGER`) FROM `xperience`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    public void testAvgMinMaxAggregateFunction() throws SQLException {
        final String query = "SELECT AVG(biginteger), MIN(biginteger), MAX(biginteger) from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, 57.57142857142857, new BigDecimal("51"), new BigDecimal("60"));
        matchSingleRowExplain(query,
                "SELECT AVG(`BIGINTEGER`), MIN(`BIGINTEGER`), MAX(`BIGINTEGER`) FROM `xperience`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    public void testCastedStringFunctions() throws SQLException {
        final String query = "select concat(upper(varcharcol),lower(repeat(varcharcol,2))) from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "TYTUtytutytu");
        matchSingleRowExplain(query,
                "SELECT CAST(CONCAT(CAST(UPPER(`VARCHARCOL`) as string),CAST(LOWER(CAST(REPEAT(`VARCHARCOL`,2) "
                        + "as string)) as string)) as string) FROM `xperience`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    public void testRewrittenDivAndModFunctions() throws SQLException {
        final String query = "select DIV(biginteger,biginteger), mod(biginteger,biginteger) from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("1"), new BigDecimal("0"));
        matchSingleRowExplain(query,
                "SELECT `BIGINTEGER` DIV `BIGINTEGER`, `BIGINTEGER` % `BIGINTEGER` FROM `xperience`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    public void testRewrittenSubStringFunction() throws SQLException {
        final String query = "select substring(stringcol FROM 1 FOR 2) from " + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "ts");
        matchSingleRowExplain(query, "SELECT SUBSTR(`STRINGCOL`, 1, 2) FROM `xperience`.`ALL_HIVE_DATA_TYPES`");
    }

    // can not test because it is supported only in newer Hive version
    public void testOrderBy() throws SQLException {
        final String query = "SELECT boolcolumn, biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES ORDER BY biginteger";
        final ResultSet result = executeQuery(query);
        matchSingleRowExplain(query,
                "SELECT `BIGINTEGER`, `BOOLCOLUMN` FROM `xperience`.`ALL_HIVE_DATA_TYPES` ORDER BY `BIGINTEGER` NULLS LAST");
    }

    // can not test because it is supported only in newer Hive version
    public void testOrderByLimit() throws SQLException {
        final String query = "SELECT boolcolumn, biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES ORDER BY biginteger LIMIT 3";
        final ResultSet result = executeQuery(query);
        matchSingleRowExplain(query,
                "SELECT `BIGINTEGER`, `BOOLCOLUMN` FROM `xperience`.`ALL_HIVE_DATA_TYPES` ORDER BY `BIGINTEGER` NULLS LAST LIMIT 3");
    }

    // can not test because it is supported only in newer Hive version
    public void testOrderByLimitOffset() throws SQLException {
        final String query = "SELECT boolcolumn, biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES ORDER BY biginteger LIMIT 2 offset 1";
        final ResultSet result = executeQuery(query);
        matchSingleRowExplain(query,
                "SELECT `BIGINTEGER`, `BOOLCOLUMN` FROM `xperience`.`ALL_HIVE_DATA_TYPES` ORDER BY `BIGINTEGER` NULLS LAST");
    }

    private static void createHiveJDBCAdapter() throws SQLException, FileNotFoundException {
        final List<String> hiveIncludes = new ArrayList<>();
        hiveIncludes.add(getConfig().getJdbcAdapterPath());
        final String jdbcPrefixPath = getConfig().getHiveJdbcPrefixPath();
        for (final String jar : getConfig().getHiveJdbcJars()) {
            hiveIncludes.add(jdbcPrefixPath + jar);
        }
        createJDBCAdapter(hiveIncludes);
    }

}
