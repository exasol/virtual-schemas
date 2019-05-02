package com.exasol.adapter.dialects.hive;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assume;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import com.exasol.adapter.dialects.IntegrationTestConfigurationCondition;
import com.exasol.adapter.dialects.hive.HiveSqlDialect;

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
                "ADAPTER.JDBC_ADAPTER", "", IS_LOCAL, getConfig().debugAddress(),
                "", null,"");
    }

    private static void createTestSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String hiveConnectionString = getConfig().getHiveJdbcConnectionString();
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (final Connection conn = DriverManager.getConnection(hiveConnectionString, "hive", ""))
        {
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

            stmt.execute("CREATE TABLE ALL_HIVE_DATA_TYPES(ARRAYCOL ARRAY<string>, BIGINTEGER BIGINT, BOOLCOLUMN BOOLEAN, CHARCOLUMN CHAR(1), DECIMALCOL DECIMAL(10,0), DOUBLECOL DOUBLE, FLOATCOL FLOAT, INTCOL INT, MAPCOL MAP<string,int>, SMALLINTEGER SMALLINT, STRINGCOL STRING, STRUCTCOL struct<a : int, b : int>, TIMESTAMPCOL TIMESTAMP, TINYINTEGER TINYINT, VARCHARCOL VARCHAR(10), BINARYCOL BINARY, DATECOL DATE)");
            stmt.execute("truncate table ALL_HIVE_DATA_TYPES");
            stmt.execute("insert into all_hive_data_types(arraycol,biginteger,boolcolumn,charcolumn,decimalcol,doublecol,floatcol,intcol,mapcol,smallinteger,stringcol,structcol,timestampcol,tinyinteger,varcharcol,binarycol,datecol) select array('etet','ettee'), 56, true, '2', 53, 56.3, 5.199999809265137, 85, map('jkljj',5), 2, 'tshg', named_struct('a',2,'b',4), timestamp '2017-01-02 13:32:50.744', 1, 'tytu', 'MTAxMA==', date '1970-01-01' from t");
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
    public void innerJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a INNER JOIN  %1$s.t2 b ON a.x=b.x", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, (long) 2, "bbb", (long) 2 ,"bbb");
        assertFalse(result.next());
    }

    @Test
    public void innerJoinWithProjection() throws SQLException {
        final String query = String.format("SELECT b.y || %1$s.t1.y FROM  %1$s.t1 INNER JOIN  %1$s.t2 b ON %1$s.t1.x=b.x", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "bbbbbb");
        assertFalse(result.next());
    }

    @Test
    public void leftJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a LEFT OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, (long) 1, "aaa", null ,null);
        matchNextRow(result, (long) 2, "bbb", (long) 2 ,"bbb");
        assertFalse(result.next());
    }

    @Test
    public void rightJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a RIGHT OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, (long) 2, "bbb", (long) 2 ,"bbb");
        matchNextRow(result, null, null, (long) 3 ,"ccc");
        assertFalse(result.next());
    }

    @Test
    public void fullOuterJoin() throws SQLException {
        final String query = String.format("SELECT * FROM  %1$s.t1 a FULL OUTER JOIN  %1$s.t2 b ON a.x=b.x ORDER BY a.x", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, (long) 1, "aaa", null ,null);
        matchNextRow(result, (long) 2, "bbb", (long) 2 ,"bbb");
        matchNextRow(result, null, null, (long) 3 ,"ccc");
        assertFalse(result.next());
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
                getSqlTimestamp(2017, 1, 2, 13, 32, 50, 744), (short) 1, "tytu", "TVRBeE1BPT0=", getSqlDate(1970, 1, 1));
    }

    @Test
    void testProjection() throws SQLException {
        final String query = "SELECT BIGINTEGER FROM " + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("56"));
        matchSingleRowExplain(query, "SELECT `BIGINTEGER` FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testRewrittenProjection() throws SQLException {
        final String query = "SELECT BINARYCOL FROM " + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "TVRBeE1BPT0=");
        matchSingleRowExplain(query, "SELECT base64(`BINARYCOL`) FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testAggregateGroupByColumn() throws SQLException {
        final String query = "SELECT boolcolumn, min(biginteger) FROM " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES GROUP BY boolcolumn";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, true, new BigDecimal("56"));
        matchSingleRowExplain(query,
                "SELECT `BOOLCOLUMN`, MIN(`BIGINTEGER`) FROM `default`.`ALL_HIVE_DATA_TYPES` GROUP BY `BOOLCOLUMN`");
    }

    @Test
    void testAggregateHaving() throws SQLException {
        final String query = "SELECT boolcolumn, min(biginteger) FROM " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES GROUP BY boolcolumn having min(biginteger)<57";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, true, new BigDecimal("56"));
        matchSingleRowExplain(query,
                "SELECT `BOOLCOLUMN`, MIN(`BIGINTEGER`) FROM `default`.`ALL_HIVE_DATA_TYPES` GROUP BY `BOOLCOLUMN` HAVING MIN(`BIGINTEGER`) < 57");
    }

    @Test
    void testComparisonPredicates() throws SQLException {
        // =, !=, <, <=, >, >=
        final String query = "select biginteger, biginteger=60, biginteger!=60, biginteger<60, biginteger<=60, biginteger>60, biginteger>=60 from "
                + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES where intcol = 85";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("56"), false, true, true, true, false, false);
        matchSingleRowExplain(query,
                "SELECT `BIGINTEGER`, `BIGINTEGER` = 60, `BIGINTEGER` <> 60, `BIGINTEGER` < 60, `BIGINTEGER` <= 60, 60 < `BIGINTEGER`,"
                        + " 60 <= `BIGINTEGER` FROM `default`.`ALL_HIVE_DATA_TYPES` WHERE `INTCOL` = 85");
    }

    @Test
    void testLogicalPredicates() throws SQLException {
        // NOT, AND, OR
        final String query = "select biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES where (biginteger < 56 or biginteger > 56) and not (biginteger is null)";
        final ResultSet result = executeQuery(query);
        assertEquals(false, result.next());
        matchSingleRowExplain(query, "SELECT `BIGINTEGER` FROM `default`.`ALL_HIVE_DATA_TYPES` "
                + "WHERE ((`BIGINTEGER` < 56 OR 56 < `BIGINTEGER`) AND NOT (`BIGINTEGER` IS NULL))");
    }

    @Test
    void testLikePredicates() throws SQLException {
        // LIKE, LIKE ESCAPE (not pushed down)
        final String query = "select varcharcol, varcharcol like 't%' escape 't' from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES where (varcharcol like 't%')";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "tytu", false);
        matchSingleRowExplain(query,
                "SELECT `VARCHARCOL` FROM `default`.`ALL_HIVE_DATA_TYPES` WHERE `VARCHARCOL` LIKE 't%'");
    }

    @Test
    void testLikePredicatesRewritten() throws SQLException {
        // REGEXP_LIKE rewritten to REGEXP
        final String query = "select varcharcol from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES where varcharcol REGEXP_LIKE 'a+'";
        final ResultSet result = executeQuery(query);
        assertEquals(false, result.next());
        matchSingleRowExplain(query,
                "SELECT `VARCHARCOL` FROM `default`.`ALL_HIVE_DATA_TYPES` WHERE `VARCHARCOL`REGEXP'a+'");
    }

    @Test
    void testMiscPredicates() throws SQLException {
        // BETWEEN, IN, IS NULL, !=NULL(rewritten to "IS NOT NULL")
        final String query = "select biginteger, biginteger in (56, 61), biginteger is null, biginteger != null from "
                + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES WHERE biginteger between 51 and 60";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("56"), true, false, true);
        matchSingleRowExplain(query, "SELECT `BIGINTEGER`, `BIGINTEGER` IN (56, 61), `BIGINTEGER` IS NULL, "
                + "`BIGINTEGER` IS NOT NULL FROM `default`.`ALL_HIVE_DATA_TYPES` WHERE `BIGINTEGER` BETWEEN 51 AND 60");
    }

    //This does not work with the current Hive version, since datatypes for the SUM columns dffer in the prepare and execute phases
    public void testCountSumAggregateFunction() throws SQLException {
        final String query = "SELECT COUNT(biginteger), COUNT(*), COUNT(DISTINCT biginteger), SUM(biginteger), SUM(DISTINCT biginteger) from "
                + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("1"), new BigDecimal("1"), new BigDecimal("1"), 56.0, 56.0);
        matchSingleRowExplain(query,
                "SELECT COUNT(`BIGINTEGER`), COUNT(*), COUNT(DISTINCT `BIGINTEGER`), SUM(`BIGINTEGER`), SUM(DISTINCT `BIGINTEGER`) FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testAvgMinMaxAggregateFunction() throws SQLException {
        final String query = "SELECT AVG(biginteger), MIN(biginteger), MAX(biginteger) from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, 56.0, new BigDecimal("56"), new BigDecimal("56"));
        matchSingleRowExplain(query,
                "SELECT AVG(`BIGINTEGER`), MIN(`BIGINTEGER`), MAX(`BIGINTEGER`) FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testCastedStringFunctions() throws SQLException {
        final String query = "select concat(upper(varcharcol),lower(repeat(varcharcol,2))) from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "TYTUtytutytu");
        matchSingleRowExplain(query,
                "SELECT CAST(CONCAT(CAST(UPPER(`VARCHARCOL`) as string),CAST(LOWER(CAST(REPEAT(`VARCHARCOL`,2) "
                        + "as string)) as string)) as string) FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testRewrittenDivAndModFunctions() throws SQLException {
        final String query = "select DIV(biginteger,biginteger), mod(biginteger,biginteger) from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, new BigDecimal("1"), new BigDecimal("0"));
        matchSingleRowExplain(query,
                "SELECT `BIGINTEGER` DIV `BIGINTEGER`, `BIGINTEGER` % `BIGINTEGER` FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    void testRewrittenSubStringFunction() throws SQLException {
        final String query = "select substring(stringcol FROM 1 FOR 2) from " + VIRTUAL_SCHEMA + ".ALL_HIVE_DATA_TYPES";
        final ResultSet result = executeQuery(query);
        matchNextRow(result, "ts");
        matchSingleRowExplain(query, "SELECT SUBSTR(`STRINGCOL`, 1, 2) FROM `default`.`ALL_HIVE_DATA_TYPES`");
    }

    @Test
    public void testOrderBy() throws SQLException {
        final String query = "SELECT boolcolumn, biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES ORDER BY biginteger";
        final ResultSet result = executeQuery(query);
        matchSingleRowExplain(query,
                "SELECT `BIGINTEGER`, `BOOLCOLUMN` FROM `default`.`ALL_HIVE_DATA_TYPES` ORDER BY `BIGINTEGER` NULLS LAST");
    }

    @Test
    public void testOrderByLimit() throws SQLException {
        final String query = "SELECT boolcolumn, biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES ORDER BY biginteger LIMIT 3";
        final ResultSet result = executeQuery(query);
        matchSingleRowExplain(query,
                "SELECT `BIGINTEGER`, `BOOLCOLUMN` FROM `default`.`ALL_HIVE_DATA_TYPES` ORDER BY `BIGINTEGER` NULLS LAST LIMIT 3");
    }

    @Test
    public void testOrderByLimitOffset() throws SQLException {
        final String query = "SELECT boolcolumn, biginteger from " + VIRTUAL_SCHEMA
                + ".ALL_HIVE_DATA_TYPES ORDER BY biginteger LIMIT 2 offset 1";
        final ResultSet result = executeQuery(query);
        matchSingleRowExplain(query,
                "SELECT `BIGINTEGER`, `BOOLCOLUMN` FROM `default`.`ALL_HIVE_DATA_TYPES` ORDER BY `BIGINTEGER` NULLS LAST");
    }

    private static void createHiveJDBCAdapter() throws SQLException, FileNotFoundException {
        final List<String> hiveIncludes = new ArrayList<>();
        hiveIncludes.add(getConfig().getJdbcAdapterPath());
        final String jdbcDriverPath = getConfig().getHiveJdbcDriverPath();
        hiveIncludes.add(jdbcDriverPath);
        createJDBCAdapter(hiveIncludes);
    }

    private static void createHiveConnection() throws SQLException,  FileNotFoundException {
        createConnection(HIVE_CONNECTION, getConfig().getHiveDockerJdbcConnectionString(), "", "");
    }
}
