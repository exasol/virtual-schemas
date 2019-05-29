package com.exasol.adapter.dialects.hive;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assume;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import com.exasol.adapter.dialects.IntegrationTestConfigurationCondition;

/**
 * Integration test for the Hive SQL dialect
 *
 * Testdata: ALL_HIVE_DATA_TYPES
 */
@ExtendWith(IntegrationTestConfigurationCondition.class)
public class HiveSqlDialectIT extends AbstractIntegrationTest {
    private static final String VIRTUAL_SCHEMA = "VS_HIVE";
    private static final String HIVE_SCHEMA = "default";
    private static final boolean IS_LOCAL = false;
    private static final String HIVE_CONNECTION = "HIVE_CONNECTION";

    @BeforeAll
    static void beforeAll() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().hiveTestsRequested());
        setConnection(connectToExa());

        createTestSchema();

        createHiveJDBCAdapter();
        createHiveConnection();
        createVirtualSchema(VIRTUAL_SCHEMA, HiveSqlDialect.getPublicName(), "", HIVE_SCHEMA, HIVE_CONNECTION, "", "",
                "ADAPTER.JDBC_ADAPTER", "", IS_LOCAL, getConfig().debugAddress(), "", null, "");
    }

    private static void createTestSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String hiveConnectionString = getConfig().getHiveJdbcConnectionString();
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (final Connection conn = DriverManager.getConnection(hiveConnectionString, "hive", "")) {
            final Statement stmt = conn.createStatement();
            stmt.execute("create table t(x int)");
            stmt.execute("truncate table t");
            stmt.execute("insert into t values (99)");

            stmt.execute("create table t1(x int, y varchar(100))");
            stmt.execute("truncate table t1");
            stmt.execute("insert into t1 values (1,'aaa'), (2,'bbb')");
            stmt.execute("create table t2(x int, y varchar(100))");
            stmt.execute("truncate table t2");
            stmt.execute("insert into t2 values (2,'bbb'), (3,'ccc')");

            stmt.execute(
                    "CREATE TABLE ALL_HIVE_DATA_TYPES(ARRAYCOL ARRAY<string>, BIGINTEGER BIGINT, BOOLCOLUMN BOOLEAN, CHARCOLUMN CHAR(1), DECIMALCOL DECIMAL(10,0), DOUBLECOL DOUBLE, FLOATCOL FLOAT, INTCOL INT, MAPCOL MAP<string,int>, SMALLINTEGER SMALLINT, STRINGCOL STRING, STRUCTCOL struct<a : int, b : int>, TIMESTAMPCOL TIMESTAMP, TINYINTEGER TINYINT, VARCHARCOL VARCHAR(10), BINARYCOL BINARY, DATECOL DATE)");
            stmt.execute("truncate table ALL_HIVE_DATA_TYPES");
            stmt.execute(
                    "insert into all_hive_data_types(arraycol,biginteger,boolcolumn,charcolumn,decimalcol,doublecol,floatcol,intcol,mapcol,smallinteger,stringcol,structcol,timestampcol,tinyinteger,varcharcol,binarycol,datecol) select array('etet','ettee'), 56, true, '2', 53, 56.3, 5.199999809265137, 85, map('jkljj',5), 2, 'tshg', named_struct('a',2,'b',4), timestamp '2017-01-02 13:32:50.744', 1, 'tytu', 'MTAxMA==', date '1970-01-01' from t");
        }
    }

    @Test
    public void testSetup() throws SQLException {
        final String query = "SELECT X FROM " + VIRTUAL_SCHEMA + ".T";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new Long("99"));
    }

    // Join Tests -------------------------------------------------------------
    @Test
    void testInnerJoin() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA + ".t1 a INNER JOIN  " + VIRTUAL_SCHEMA
                + ".t2 b ON a.x=b.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> matchNextRow(result, 2L, "bbb", 2L, "bbb"), () -> assertFalse(result.next()));
    }

    @Test
    void testInnerJoinWithProjection() throws SQLException {
        final String query = "SELECT b.y || " + VIRTUAL_SCHEMA + ".t1.y FROM  " + VIRTUAL_SCHEMA + ".t1 INNER JOIN  "
                + VIRTUAL_SCHEMA + ".t2 b ON " + VIRTUAL_SCHEMA + ".t1.x=b.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> matchNextRow(result, "bbbbbb"), () -> assertFalse(result.next()));
    }

    @Test
    void testLeftJoin() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA + ".t1 a LEFT OUTER JOIN  " + VIRTUAL_SCHEMA
                + ".t2 b ON a.x=b.x ORDER BY a.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> matchNextRow(result, 1L, "aaa", null, null), () -> matchNextRow(result, 2L, "bbb", 2L, "bbb"),
                () -> assertFalse(result.next()));
    }

    @Test
    void testRightJoin() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA + ".t1 a RIGHT OUTER JOIN  " + VIRTUAL_SCHEMA
                + ".t2 b ON a.x=b.x ORDER BY a.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> matchNextRow(result, 2L, "bbb", 2L, "bbb"), () -> matchNextRow(result, null, null, 3L, "ccc"),
                () -> assertFalse(result.next()));
    }

    @Test
    void testFullOuterJoin() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA + ".t1 a FULL OUTER JOIN  " + VIRTUAL_SCHEMA
                + ".t2 b ON a.x=b.x ORDER BY a.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> matchNextRow(result, 1L, "aaa", null, null), () -> matchNextRow(result, 2L, "bbb", 2L, "bbb"),
                () -> matchNextRow(result, null, null, 3L, "ccc"), () -> assertFalse(result.next()));
    }

    @Test
    void testRightJoinWithComplexCondition() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA + ".t1 a RIGHT OUTER JOIN  " + VIRTUAL_SCHEMA
                + ".t2 b ON a.x||a.y=b.x||b.y ORDER BY a.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> matchNextRow(result, 2L, "bbb", 2L, "bbb"), () -> matchNextRow(result, null, null, 3L, "ccc"),
                () -> assertFalse(result.next()));
    }

    @Test
    void testFullOuterJoinWithComplexCondition() throws SQLException {
        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA + ".t1 a FULL OUTER JOIN  " + VIRTUAL_SCHEMA
                + ".t2 b ON a.x-b.x=0 ORDER BY a.x";
        final ResultSet result = executeQuery(query);
        assertAll(() -> matchNextRow(result, 1L, "aaa", null, null), () -> matchNextRow(result, 2L, "bbb", 2L, "bbb"),
                () -> matchNextRow(result, null, null, 3L, "ccc"), () -> assertFalse(result.next()));
    }

    @Test
    void testTypeMapping() throws SQLException {
        final ResultSet result = executeQuery(
                "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, COLUMN_DEFAULT FROM EXA_DBA_COLUMNS WHERE COLUMN_SCHEMA = '"
                        + VIRTUAL_SCHEMA + "' AND COLUMN_TABLE='ALL_HIVE_DATA_TYPES' ORDER BY COLUMN_ORDINAL_POSITION");
        matchNextRow(result, "ARRAYCOL", "VARCHAR(255) ASCII", (long) 255, null, null, null);
        matchNextRow(result, "BIGINTEGER", "DECIMAL(19,0)", (long) 19, (long) 19, (long) 0, null);
        matchNextRow(result, "BOOLCOLUMN", "BOOLEAN", (long) 1, null, null, null);
        matchNextRow(result, "CHARCOLUMN", "CHAR(1) ASCII", (long) 1, null, null, null);
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
        matchNextRow(result, "VARCHARCOL", "VARCHAR(10) ASCII", (long) 10, null, null, null);
        matchNextRow(result, "BINARYCOL", "VARCHAR(2000000) UTF8", (long) 2000000, null, null, null);
        matchLastRow(result, "DATECOL", "DATE", (long) 10, null, null, null);
    }

    @Test
    void testSelectWithAllTypes() throws SQLException {
        final ResultSet result = executeQuery("SELECT * from " + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES");
        matchNextRow(result, "[\"etet\",\"ettee\"]", new BigDecimal("56"), true, "2", (long) 53, 56.3,
                5.199999809265137, (long) 85, "{\"jkljj\":5}", 2, "tshg", "{\"a\":2,\"b\":4}",
                getSqlTimestamp(2017, 1, 2, 13, 32, 50, 744), (short) 1, "tytu", "TVRBeE1BPT0=",
                getSqlDate(1970, 1, 1));
    }

    @Test
    void testProjection() throws SQLException {
        final String query = "SELECT BIGINTEGER FROM " + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("56"));
        matchSingleRowExplain(query, "SELECT `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testRewrittenProjection() throws SQLException {
        final String query = "SELECT BINARYCOL FROM " + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "TVRBeE1BPT0=");
        matchSingleRowExplain(query,
                "SELECT base64(`ALL_HIVE_DATA_TYPES`.`BINARYCOL`) FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testAggregateGroupByColumn() throws SQLException {
        final String query = "SELECT boolcolumn, min(biginteger) FROM " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES GROUP BY boolcolumn";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, true, new BigDecimal("56"));
        matchSingleRowExplain(query,
                "SELECT `ALL_HIVE_DATA_TYPES`.`BOOLCOLUMN`, MIN(`ALL_HIVE_DATA_TYPES`.`BIGINTEGER`) FROM `default`.`ALL_HIVE_DATA_TYPES` GROUP BY `ALL_HIVE_DATA_TYPES`.`BOOLCOLUMN`");
    }

    @Test
    void testAggregateHaving() throws SQLException {
        final String query = "SELECT boolcolumn, min(biginteger) FROM " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES GROUP BY boolcolumn having min(biginteger)<57";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, true, new BigDecimal("56"));
        matchSingleRowExplain(query,
                "SELECT `ALL_HIVE_DATA_TYPES`.`BOOLCOLUMN`, MIN(`ALL_HIVE_DATA_TYPES`.`BIGINTEGER`) FROM `default`.`ALL_HIVE_DATA_TYPES` GROUP BY `ALL_HIVE_DATA_TYPES`.`BOOLCOLUMN` HAVING MIN(`ALL_HIVE_DATA_TYPES`.`BIGINTEGER`) < 57");
    }

    @Test
    void testComparisonPredicates() throws SQLException {
        // =, !=, <, <=, >, >=
        final String query = "select biginteger, biginteger=60, biginteger!=60, biginteger<60, biginteger<=60, biginteger>60, biginteger>=60 from "
                + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES where intcol = 85";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("56"), false, true, true, true, false, false);
        matchSingleRowExplain(query,
                "SELECT `ALL_HIVE_DATA_TYPES`.`BIGINTEGER`, `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` = 60, `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` <> 60, `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` < 60, `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` <= 60, 60 < `ALL_HIVE_DATA_TYPES`.`BIGINTEGER`,"
                        + " 60 <= `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` FROM `default`.`ALL_HIVE_DATA_TYPES` WHERE `ALL_HIVE_DATA_TYPES`.`INTCOL` = 85");
    }

    @Test
    void testLogicalPredicates() throws SQLException {
        // NOT, AND, OR
        final String query = "select biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES where (biginteger < 56 or biginteger > 56) and not (biginteger is null)";
        final ResultSet result = executeQuery(query);
        assertEquals(false, result.next());
        matchSingleRowExplain(query, "SELECT `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` FROM `default`.`ALL_HIVE_DATA_TYPES` "
                + "WHERE ((`ALL_HIVE_DATA_TYPES`.`BIGINTEGER` < 56 OR 56 < `ALL_HIVE_DATA_TYPES`.`BIGINTEGER`) AND NOT (`ALL_HIVE_DATA_TYPES`.`BIGINTEGER` IS NULL))");
    }

    @Test
    void testLikePredicates() throws SQLException {
        // LIKE, LIKE ESCAPE (not pushed down)
        final String query = "select varcharcol, varcharcol like 't%' escape 't' from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES where (varcharcol like 't%')";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "tytu", false);
        matchSingleRowExplain(query,
                "SELECT `ALL_HIVE_DATA_TYPES`.`VARCHARCOL` FROM `default`.`ALL_HIVE_DATA_TYPES` WHERE `ALL_HIVE_DATA_TYPES`.`VARCHARCOL` LIKE 't%'");
    }

    @Test
    void testLikePredicatesRewritten() throws SQLException {
        // REGEXP_LIKE rewritten to REGEXP
        final String query = "select varcharcol from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES where varcharcol REGEXP_LIKE 'a+'";
        final ResultSet result = executeQuery(query);
        assertEquals(false, result.next());
        matchSingleRowExplain(query,
                "SELECT `ALL_HIVE_DATA_TYPES`.`VARCHARCOL` FROM `default`.`ALL_HIVE_DATA_TYPES` WHERE `ALL_HIVE_DATA_TYPES`.`VARCHARCOL`REGEXP'a+'");
    }

    @Test
    void testMiscPredicates() throws SQLException {
        // BETWEEN, IN, IS NULL, !=NULL(rewritten to "IS NOT NULL")
        final String query = "select biginteger, biginteger in (56, 61), biginteger is null, biginteger != null from "
                + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES WHERE biginteger between 51 and 60";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("56"), true, false, true);
        matchSingleRowExplain(query,
                "SELECT `ALL_HIVE_DATA_TYPES`.`BIGINTEGER`, `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` IN (56, 61), `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` IS NULL, "
                        + "`ALL_HIVE_DATA_TYPES`.`BIGINTEGER` IS NOT NULL FROM `default`.`ALL_HIVE_DATA_TYPES` WHERE `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` BETWEEN 51 AND 60");
    }

    // This does not work with the current Hive version, since datatypes for the SUM
    // columns dffer in the prepare and execute phases
    public void testCountSumAggregateFunction() throws SQLException {
        final String query = "SELECT COUNT(biginteger), COUNT(*), COUNT(DISTINCT biginteger), SUM(biginteger), SUM(DISTINCT biginteger) from "
                + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("1"), new BigDecimal("1"), new BigDecimal("1"), 56.0, 56.0);
        matchSingleRowExplain(query,
                "SELECT COUNT(`ALL_HIVE_DATA_TYPES`.`BIGINTEGER`), COUNT(*), COUNT(DISTINCT `ALL_HIVE_DATA_TYPES`.`BIGINTEGER`), SUM(`ALL_HIVE_DATA_TYPES`.`BIGINTEGER`), SUM(DISTINCT `ALL_HIVE_DATA_TYPES`.`BIGINTEGER`) FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testAvgMinMaxAggregateFunction() throws SQLException {
        final String query = "SELECT AVG(biginteger), MIN(biginteger), MAX(biginteger) from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, 56.0, new BigDecimal("56"), new BigDecimal("56"));
        matchSingleRowExplain(query,
                "SELECT AVG(`ALL_HIVE_DATA_TYPES`.`BIGINTEGER`), MIN(`ALL_HIVE_DATA_TYPES`.`BIGINTEGER`), MAX(`ALL_HIVE_DATA_TYPES`.`BIGINTEGER`) FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testCastedStringFunctions() throws SQLException {
        final String query = "select concat(upper(varcharcol),lower(repeat(varcharcol,2))) from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "TYTUtytutytu");
        matchSingleRowExplain(query,
                "SELECT CAST(CONCAT(CAST(UPPER(`ALL_HIVE_DATA_TYPES`.`VARCHARCOL`) as string),CAST(LOWER(CAST(REPEAT(`ALL_HIVE_DATA_TYPES`.`VARCHARCOL`,2) "
                        + "as string)) as string)) as string) FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testRewrittenDivAndModFunctions() throws SQLException {
        final String query = "select DIV(biginteger,biginteger), mod(biginteger,biginteger) from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("1"), new BigDecimal("0"));
        matchSingleRowExplain(query,
                "SELECT `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` DIV `ALL_HIVE_DATA_TYPES`.`BIGINTEGER`, `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` % `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testRewrittenSubStringFunction() throws SQLException {
        final String query = "select substring(stringcol FROM 1 FOR 2) from " + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "ts");
        matchSingleRowExplain(query,
                "SELECT SUBSTR(`ALL_HIVE_DATA_TYPES`.`STRINGCOL`, 1, 2) FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    public void testOrderBy() throws SQLException {
        final String query = "SELECT boolcolumn, biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES ORDER BY biginteger";
        executeQuery(query);
        matchSingleRowExplain(query,
                "SELECT `ALL_HIVE_DATA_TYPES`.`BIGINTEGER`, `ALL_HIVE_DATA_TYPES`.`BOOLCOLUMN` FROM `default`.`ALL_HIVE_DATA_TYPES` ORDER BY `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` NULLS LAST");
    }

    @Test
    public void testOrderByLimit() throws SQLException {
        final String query = "SELECT boolcolumn, biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES ORDER BY biginteger LIMIT 3";
        executeQuery(query);
        matchSingleRowExplain(query,
                "SELECT `ALL_HIVE_DATA_TYPES`.`BIGINTEGER`, `ALL_HIVE_DATA_TYPES`.`BOOLCOLUMN` FROM `default`.`ALL_HIVE_DATA_TYPES` ORDER BY `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` NULLS LAST LIMIT 3");
    }

    @Test
    public void testOrderByLimitOffset() throws SQLException {
        final String query = "SELECT boolcolumn, biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES ORDER BY biginteger LIMIT 2 offset 1";
        executeQuery(query);
        matchSingleRowExplain(query,
                "SELECT `ALL_HIVE_DATA_TYPES`.`BIGINTEGER`, `ALL_HIVE_DATA_TYPES`.`BOOLCOLUMN` FROM `default`.`ALL_HIVE_DATA_TYPES` ORDER BY `ALL_HIVE_DATA_TYPES`.`BIGINTEGER` NULLS LAST");
    }

    private static void createHiveJDBCAdapter() throws SQLException, FileNotFoundException {
        final List<String> hiveIncludes = new ArrayList<>();
        hiveIncludes.add(getConfig().getJdbcAdapterPath());
        final String jdbcDriverPath = getConfig().getHiveJdbcDriverPath();
        hiveIncludes.add(jdbcDriverPath);
        createJDBCAdapter(hiveIncludes);
    }

    private static void createHiveConnection() throws SQLException, FileNotFoundException {
        createConnection(HIVE_CONNECTION, getConfig().getHiveDockerJdbcConnectionString(), "", "");
    }
}
