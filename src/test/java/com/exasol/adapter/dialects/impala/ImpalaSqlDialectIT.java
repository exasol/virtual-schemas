package com.exasol.adapter.dialects.impala;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assume;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import com.exasol.adapter.dialects.IntegrationTestConfigurationCondition;

/**
 * Integration test for the Impala SQL dialect
 *
 * Testdata: sample_07: code string, description string, total_emp int, salary int
 */
@Tag("integration")
@ExtendWith(IntegrationTestConfigurationCondition.class)
public class ImpalaSqlDialectIT extends AbstractIntegrationTest {
//    private static final String VIRTUAL_SCHEMA = "VS_IMPALA";
//    private static final String IMPALA_SCHEMA = "default";
//    private static final boolean IS_LOCAL = false;
//
//    @BeforeAll
//    static void beforeAll() throws FileNotFoundException, SQLException, ClassNotFoundException {
//        Assume.assumeTrue(getConfig().impalaTestsRequested());
//        setConnection(connectToExa());
//        createImpalaJDBCAdapter();
//        createVirtualSchema(VIRTUAL_SCHEMA, ImpalaSqlDialect.NAME, "", IMPALA_SCHEMA, "", "no-user", "no-password",
//                "ADAPTER.JDBC_ADAPTER", getConfig().getImpalaJdbcConnectionString(), IS_LOCAL,
//                getConfig().debugAddress(), "SAMPLE_07,ALL_HIVE_IMPALA_TYPES,SIMPLE,SIMPLE_WITH_NULLS", null, "");
//    }
//
//    @Test
//    void testTypeMapping() throws SQLException {
//        final ResultSet result = executeQuery(
//                "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, COLUMN_DEFAULT FROM EXA_DBA_COLUMNS WHERE COLUMN_SCHEMA = '"
//                        + VIRTUAL_SCHEMA
//                        + "' AND COLUMN_TABLE='ALL_HIVE_IMPALA_TYPES' ORDER BY COLUMN_ORDINAL_POSITION");
//        assertNextRow(result, "C1", "DECIMAL(3,0)", 3L, 3L, 0L, null);
//        assertNextRow(result, "C2", "DECIMAL(5,0)", 5L, 5L, 0L, null);
//        assertNextRow(result, "C3", "DECIMAL(10,0)", (long) 10, (long) 10, (long) 0, null);
//        assertNextRow(result, "C4", "DECIMAL(19,0)", (long) 19, (long) 19, (long) 0, null);
//        assertNextRow(result, "C5", "DOUBLE", (long) 64, null, null, null);
//        assertNextRow(result, "C6", "DOUBLE", (long) 64, null, null, null);
//        assertNextRow(result, "C7", "DECIMAL(9,0)", (long) 9, (long) 9, (long) 0, null);
//        assertNextRow(result, "C8", "DECIMAL(12,2)", (long) 12, (long) 12, (long) 2, null);
//        assertNextRow(result, "C9", "VARCHAR(2000000) UTF8", (long) 2000000, null, null, null);
//        assertNextRow(result, "C10", "TIMESTAMP", (long) 29, null, null, null);
//        // Impala has problems with STRING data type, and probably automatically
//        // restricts it to ASCII (otherwise several operations don't work with the
//        // column)
//        // See
//        // http://www.cloudera.com/documentation/enterprise/5-5-x/topics/impala_string.html
//        assertNextRow(result, "C11", "VARCHAR(32767) ASCII", (long) 32767, null, null, null);
//        assertNextRow(result, "C12", "VARCHAR(1000) UTF8", (long) 1000, null, null, null);
//        assertNextRow(result, "C13", "CHAR(10) UTF8", (long) 10, null, null, null);
//        assertLastRow(result, "C14", "BOOLEAN", (long) 1, null, null, null);
//    }
//
//    @Test
//    void testSelectWithAllTypes() throws SQLException {
//        final ResultSet result = executeQuery("SELECT * from " + VIRTUAL_SCHEMA + ".ALL_HIVE_IMPALA_TYPES");
//        assertLastRow(result, (short) 123, 12345, 1234567890L, new BigDecimal(1234567890123456789L), 12.199999809265137,
//                12.2, 12345, new BigDecimal("12345.12"), "12345.12", getSqlTimestamp(1985, 9, 25, 17, 45, 30, 5), "abc",
//                "varchar 茶", "char 茶  ", // 茶 requires 3 bytes, and char(10) means 10 bytes for Impala.
//                true);
//    }
//
//    @Test
//    void testSimpleQuery() throws SQLException {
//        final String query = "SELECT * FROM sample_07";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, "00-0000", "All Occupations", (long) 134354250, (long) 40690);
//    }
//
//    @Test
//    void testProjection() throws SQLException {
//        final String query = "SELECT c2 FROM " + VIRTUAL_SCHEMA + ".ALL_HIVE_IMPALA_TYPES";
//        final ResultSet result = executeQuery(query);
//        assertLastRow(result, 12345);
//        matchSingleRowExplain(query, "SELECT `C2` FROM `default`.`ALL_HIVE_IMPALA_TYPES`");
//    }
//
//    @Test
//    void testComparisonPredicates() throws SQLException {
//        // =, !=, <, <=, >, >=
//        final String query = "select salary, salary=33880, salary!=33880, salary<33880, salary<=33880, salary>33880, salary>=33880 from "
//                + VIRTUAL_SCHEMA + ".sample_07 where code = '11-1031'";
//        final ResultSet result = executeQuery(query);
//        assertLastRow(result, 33880L, true, false, false, true, false, true);
//        matchSingleRowExplain(query,
//                "SELECT `SALARY`, `SALARY` = 33880, `SALARY` != 33880, `SALARY` < 33880, `SALARY` <= 33880, 33880 < `SALARY`, 33880 <= `SALARY` FROM `default`.`SAMPLE_07` WHERE `CODE` = '11-1031'");
//    }
//
//    @Test
//    void testLogicalPredicates() throws SQLException {
//        // NOT, AND, OR
//        final String query = "select * from vs_impala.simple_with_nulls where (c1 < 2 or c1 > 2) and not (c2 is null)";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, 1L, "a");
//        assertLastRow(result, 3L, "b");
//        matchSingleRowExplain(query,
//                "SELECT * FROM `default`.`SIMPLE_WITH_NULLS` WHERE ((`C1` < 2 OR 2 < `C1`) AND NOT (`C2` IS NULL))");
//    }
//
//    @Test
//    void testLikePredicates() throws SQLException {
//        // LIKE, LIKE ESCAPE (not pushed down), REGEXP_LIKE
//        final String query = "select code, code like 'x%1' escape 'x' from " + VIRTUAL_SCHEMA
//                + ".sample_07 where (code like '15%' and not code regexp_like '.*1$')";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, "15-0000", false);
//        assertNextRow(result, "15-1032", false);
//        assertNextRow(result, "15-1099", false);
//        assertLastRow(result, "15-2099", false);
//        matchSingleRowExplain(query,
//                "SELECT `CODE` FROM `default`.`SAMPLE_07` WHERE (`CODE` LIKE '15%' AND NOT (`CODE` REGEXP '.*1$'))");
//    }
//
//    @Test
//    void testMiscPredicates() throws SQLException {
//        // BETWEEN, IN, IS NULL, IS NOT NULL
//        final String query = "select c1, c2, c1 in (2, 3), c2 is null, c2 is not null from vs_impala.simple_with_nulls WHERE c1 between 1 and 2";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, 1L, "a", false, false, true);
//        assertNextRow(result, 2L, null, true, true, false);
//        assertLastRow(result, 1L, null, false, true, false);
//        matchSingleRowExplain(query,
//                "SELECT `C1`, `C2`, `C1` IN (2, 3), `C2` IS NULL, `C2` IS NOT NULL FROM `default`.`SIMPLE_WITH_NULLS` WHERE `C1` BETWEEN 1 AND 2");
//    }
//
//    @Test
//    void testCountSumAggregateFunction() throws SQLException {
//        final String query = "SELECT COUNT(A), COUNT(*), COUNT(DISTINCT A), SUM(A), SUM(DISTINCT A) from vs_impala.simple";
//        final ResultSet result = executeQuery(query);
//        assertLastRow(result, new BigDecimal(6), new BigDecimal(6), new BigDecimal(3), 12D, 6D);
//        matchSingleRowExplain(query,
//                "SELECT COUNT(`A`), COUNT(*), COUNT(DISTINCT `A`), CAST(SUM(`A`) AS DOUBLE), CAST(SUM(DISTINCT `A`) AS DOUBLE) FROM `default`.`SIMPLE`");
//    }
//
//    public void testAvgMinMaxAggregateFunction() throws SQLException {
//        final String query = "SELECT AVG(C), MIN(A), MIN(DISTINCT A), MAX(A), MAX(DISTINCT A) from VS_IMPALA.SIMPLE";
//        final ResultSet result = executeQuery(query);
//        assertLastRow(result, new BigDecimal(3.85), new BigDecimal(1), new BigDecimal(1), new BigDecimal(3),
//                new BigDecimal(3));
//        matchSingleRowExplain(query, "SELECT AVG(`C`), MIN(`A`), MIN(`A`), MAX(`A`), MAX(`A`) FROM `default`.`SIMPLE`");
//    }
//
//    @Test
//    void testLiteralsPredicates() throws SQLException {
//        // String/varchar, bool, null, double, decimal
//        final String query = "select count(*) from vs_impala.ALL_HIVE_IMPALA_TYPES where c11 = 'abc' and c12 = 'varchar 茶' and c6 = 1.22E1 and c8 = 12345.12";
//        final ResultSet result = executeQuery(query);
//        assertLastRow(result, new BigDecimal(1));
//        matchSingleRowExplain(query,
//                "SELECT COUNT(*) FROM `default`.`ALL_HIVE_IMPALA_TYPES` WHERE (`C11` = 'abc' AND `C12` = 'varchar 茶' AND `C6` = 12.2 AND `C8` = 12345.12)");
//    }
//
//    @Test
//    void testOrderBy() throws SQLException {
//        final String query = "SELECT CODE, SALARY from sample_07 ORDER BY SALARY";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, "35-3021", 16700L);
//        matchSingleRowExplain(query, "SELECT `CODE`, `SALARY` FROM `default`.`SAMPLE_07` ORDER BY `SALARY`");
//    }
//
//    @Test
//    void testOrderByLimit() throws SQLException {
//        final String query = "SELECT CODE, SALARY from sample_07 ORDER BY SALARY LIMIT 3";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, "35-3021", 16700L);
//        assertNextRow(result, "35-2011", 16860L);
//        assertLastRow(result, "35-9021", 17060L);
//        matchSingleRowExplain(query, "SELECT `CODE`, `SALARY` FROM `default`.`SAMPLE_07` ORDER BY `SALARY` LIMIT 3");
//    }
//
//    @Test
//    void testOrderByLimitOffset() throws SQLException {
//        final String query = "SELECT CODE, SALARY from sample_07 ORDER BY SALARY LIMIT 2 OFFSET 1";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, "35-2011", 16860L);
//        assertLastRow(result, "35-9021", 17060L);
//        matchSingleRowExplain(query,
//                "SELECT `CODE`, `SALARY` FROM `default`.`SAMPLE_07` ORDER BY `SALARY` LIMIT 2 OFFSET 1");
//    }
//
//    @Test
//    void testAggregateFunctions() throws SQLException {
//        final String query = "SELECT count(*), count(salary), count(distinct salary) FROM sample_07";
//        final ResultSet result = executeQuery(query);
//        assertLastRow(result, new BigDecimal(823), new BigDecimal(819), new BigDecimal(759));
//        matchSingleRowExplain(query,
//                "SELECT COUNT(*), COUNT(`SALARY`), COUNT(DISTINCT `SALARY`) FROM `default`.`SAMPLE_07`");
//    }
//
//    private static void createImpalaJDBCAdapter() throws SQLException, FileNotFoundException {
//        final List<String> impalaIncludes = new ArrayList<>();
//        impalaIncludes.add(getConfig().getJdbcAdapterPath());
//        final String jdbcPrefixPath = getConfig().getImpalaJdbcPrefixPath();
//        for (final String jar : getConfig().getImpalaJdbcJars()) {
//            impalaIncludes.add(jdbcPrefixPath + jar);
//        }
//        createJDBCAdapter(impalaIncludes);
//    }
}
