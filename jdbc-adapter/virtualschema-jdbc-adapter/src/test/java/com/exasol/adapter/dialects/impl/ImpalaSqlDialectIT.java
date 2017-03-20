package com.exasol.adapter.dialects.impl;


import com.exasol.adapter.dialects.AbstractIntegrationTest;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Integration test for the Impala SQL dialect
 *
 * Testdata: sample_07: code string, description string, total_emp int, salary int
 */
public class ImpalaSqlDialectIT extends AbstractIntegrationTest {

    private static final String VIRTUAL_SCHEMA = "VS_IMPALA";
    private static final String IMPALA_SCHEMA = "default";
    private static final boolean IS_LOCAL = false;
    
    @BeforeClass
    public static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().impalaTestsRequested());
        setConnection(connectToExa());

        createImpalaJDBCAdapter();
        createVirtualSchema(
                VIRTUAL_SCHEMA,
                ImpalaSqlDialect.NAME,
                "", IMPALA_SCHEMA,
                "",
                "no-user",
                "no-password",
                "ADAPTER.JDBC_ADAPTER",
                getConfig().getImpalaJdbcConnectionString(),
                IS_LOCAL,
                getConfig().debugAddress(),
                "SAMPLE_07,ALL_HIVE_IMPALA_TYPES,SIMPLE,SIMPLE_WITH_NULLS");
    }

    @Test
    public void testTypeMapping() throws SQLException, ClassNotFoundException, FileNotFoundException {
        // TODO Test type mapping for tables with invalid Impala Types
        ResultSet result = executeQuery("SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, COLUMN_DEFAULT FROM EXA_DBA_COLUMNS WHERE COLUMN_SCHEMA = '" + VIRTUAL_SCHEMA + "' AND COLUMN_TABLE='ALL_HIVE_IMPALA_TYPES' ORDER BY COLUMN_ORDINAL_POSITION");
        matchNextRow(result, "C1", "DECIMAL(3,0)", 3L, 3L, 0L, null);
        matchNextRow(result, "C2", "DECIMAL(5,0)", 5L, 5L, 0L, null);
        matchNextRow(result, "C3", "DECIMAL(10,0)", (long)10, (long)10, (long)0, null);
        matchNextRow(result, "C4", "DECIMAL(19,0)", (long)19, (long)19, (long)0, null);
        matchNextRow(result, "C5", "DOUBLE", (long)64, null, null, null);
        matchNextRow(result, "C6", "DOUBLE", (long)64, null, null, null);
        matchNextRow(result, "C7", "DECIMAL(9,0)", (long)9, (long)9, (long)0, null);
        matchNextRow(result, "C8", "DECIMAL(12,2)", (long)12, (long)12, (long)2, null);
        matchNextRow(result, "C9", "VARCHAR(2000000) UTF8", (long)2000000, null, null, null);
        matchNextRow(result, "C10", "TIMESTAMP", (long)29, null, null, null);
        // Impala has problems with STRING data type, and probably automatically restricts it to ASCII (otherwise several operations don't work with the column)
        // See http://www.cloudera.com/documentation/enterprise/5-5-x/topics/impala_string.html
        matchNextRow(result, "C11", "VARCHAR(32767) ASCII", (long)32767, null, null, null);
        matchNextRow(result, "C12", "VARCHAR(1000) UTF8", (long)1000, null, null, null);
        matchNextRow(result, "C13", "CHAR(10) UTF8", (long)10, null, null, null);
        matchLastRow(result, "C14", "BOOLEAN", (long)1, null, null, null);
    }

    @Test
    public void testSelectWithAllTypes() throws SQLException {
        ResultSet result = executeQuery("SELECT * from " + VIRTUAL_SCHEMA + ".ALL_HIVE_IMPALA_TYPES");
        matchLastRow(result,
                (short)123,
                12345,
                1234567890L,
                new BigDecimal(1234567890123456789L),
                12.199999809265137,
                12.2,
                12345,
                new BigDecimal("12345.12"),
                "12345.12",getSqlTimestamp(1985, 9, 25, 17, 45, 30, 5),
                "abc",
                "varchar 茶",
                "char 茶  ", // 茶 requires 3 bytes, and char(10) means 10 bytes for Impala.
                true);
    }

    @Test
    public void testSimpleQuery() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT * FROM sample_07";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "00-0000", "All Occupations", (long)134354250, (long)40690);
    }

    @Test
    public void testProjection() throws SQLException {
        String query = "SELECT c2 FROM " + VIRTUAL_SCHEMA + ".ALL_HIVE_IMPALA_TYPES";
        ResultSet result = executeQuery(query);
        matchLastRow(result, 12345);
        matchSingleRowExplain(query, "SELECT `C2` FROM `default`.`ALL_HIVE_IMPALA_TYPES`");
    }

    @Test
    public void testComparisonPredicates() throws SQLException {
        // =, !=, <, <=, >, >=
        String query = "select salary, salary=33880, salary!=33880, salary<33880, salary<=33880, salary>33880, salary>=33880 from " + VIRTUAL_SCHEMA + ".sample_07 where code = '11-1031'";
        ResultSet result = executeQuery(query);
        matchLastRow(result, 33880L, true, false, false, true, false, true);
        matchSingleRowExplain(query, "SELECT `SALARY`, `SALARY` = 33880, `SALARY` != 33880, `SALARY` < 33880, `SALARY` <= 33880, 33880 < `SALARY`, 33880 <= `SALARY` FROM `default`.`SAMPLE_07` WHERE `CODE` = '11-1031'");
    }

    @Test
    public void testLogicalPredicates() throws SQLException {
        // NOT, AND, OR
        String query = "select * from vs_impala.simple_with_nulls where (c1 < 2 or c1 > 2) and not (c2 is null)";
        ResultSet result = executeQuery(query);
        matchNextRow(result, 1L, "a");
        matchLastRow(result, 3L, "b");
        matchSingleRowExplain(query, "SELECT * FROM `default`.`SIMPLE_WITH_NULLS` WHERE ((`C1` < 2 OR 2 < `C1`) AND NOT (`C2` IS NULL))");
    }

    @Test
    public void testLikePredicates() throws SQLException {
        // LIKE, LIKE ESCAPE (not pushed down), REGEXP_LIKE
        String query = "select code, code like 'x%1' escape 'x' from " + VIRTUAL_SCHEMA + ".sample_07 where (code like '15%' and not code regexp_like '.*1$')";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "15-0000", false);
        matchNextRow(result, "15-1032", false);
        matchNextRow(result, "15-1099", false);
        matchLastRow(result, "15-2099", false);
        matchSingleRowExplain(query, "SELECT `CODE` FROM `default`.`SAMPLE_07` WHERE (`CODE` LIKE '15%' AND NOT (`CODE` REGEXP '.*1$'))");
    }

    @Test
    public void testMiscPredicates() throws SQLException {
        // BETWEEN, IN, IS NULL, IS NOT NULL
        String query = "select c1, c2, c1 in (2, 3), c2 is null, c2 is not null from vs_impala.simple_with_nulls WHERE c1 between 1 and 2";
        ResultSet result = executeQuery(query);
        matchNextRow(result, 1L, "a", false, false, true);
        matchNextRow(result, 2L, null, true, true, false);
        matchLastRow(result, 1L, null, false, true, false);
        matchSingleRowExplain(query, "SELECT `C1`, `C2`, `C1` IN (2, 3), `C2` IS NULL, `C2` IS NOT NULL FROM `default`.`SIMPLE_WITH_NULLS` WHERE `C1` BETWEEN 1 AND 2");
    }

    @Test
    public void testCountSumAggregateFunction() throws SQLException {
        String query = "SELECT COUNT(A), COUNT(*), COUNT(DISTINCT A), SUM(A), SUM(DISTINCT A) from vs_impala.simple";
        ResultSet result = executeQuery(query);
        matchLastRow(result, new BigDecimal(6), new BigDecimal(6), new BigDecimal(3), 12D, 6D);
        matchSingleRowExplain(query, "SELECT COUNT(`A`), COUNT(*), COUNT(DISTINCT `A`), CAST(SUM(`A`) AS DOUBLE), CAST(SUM(DISTINCT `A`) AS DOUBLE) FROM `default`.`SIMPLE`");
    }

    public void testAvgMinMaxAggregateFunction() throws SQLException {
        String query = "SELECT AVG(C), MIN(A), MIN(DISTINCT A), MAX(A), MAX(DISTINCT A) from VS_IMPALA.SIMPLE";
        ResultSet result = executeQuery(query);
        matchLastRow(result, new BigDecimal(3.85), new BigDecimal(1), new BigDecimal(1), new BigDecimal(3), new BigDecimal(3));
        matchSingleRowExplain(query, "SELECT AVG(`C`), MIN(`A`), MIN(`A`), MAX(`A`), MAX(`A`) FROM `default`.`SIMPLE`");
    }

    @Test
    public void testLiteralsPredicates() throws SQLException {
        // String/varchar, bool, null, double, decimal
        String query = "select count(*) from vs_impala.ALL_HIVE_IMPALA_TYPES where c11 = 'abc' and c12 = 'varchar 茶' and c6 = 1.22E1 and c8 = 12345.12";
        ResultSet result = executeQuery(query);
        matchLastRow(result, new BigDecimal(1));
        matchSingleRowExplain(query, "SELECT COUNT(*) FROM `default`.`ALL_HIVE_IMPALA_TYPES` WHERE (`C11` = 'abc' AND `C12` = 'varchar 茶' AND `C6` = 12.2 AND `C8` = 12345.12)");
    }

    @Test
    public void testAggregationFunctions() throws SQLException {
        /**
         * COUNT(A)
         COUNT(*)
         COUNT(DISTINCT A)
         COUNT(ALL (A, C))
         COVAR_POP(A, C)
         COVAR_SAMP(A, C)
         FIRST_VALUE(A)
         GROUP_CONCAT(A)
         GROUP_CONCAT(DISTINCT A)
         GROUP_CONCAT(A ORDER BY C)
         GROUP_CONCAT(A ORDER BY C DESC)
         GROUP_CONCAT(A SEPARATOR
         GROUPING(A)
         GROUPING(A, C)
         GROUPING_ID(A)
         GROUPING_ID(A, C)
         LAST_VALUE(A)
         MAX(A)
         MAX(ALL A)
         MAX(DISTINCT A)
         MEDIAN(A)
         MIN(A)
         MIN(ALL A)
         MIN(DISTINCT A)
         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY A)
         PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY A)
         REGR_AVGX(A, C)
         REGR_AVGY(A, C)
         REGR_COUNT(A, C)
         REGR_INTERCEPT(A, C)
         REGR_R2(A, C)
         REGR_SLOPE(A, C)
         REGR_SXX(A, C)
         REGR_SXY(A, C)
         REGR_SYY(A, C)
         STDDEV(A)
         STDDEV(ALL A)
         STDDEV(DISTINCT A)
         STDDEV_POP(A)
         STDDEV_POP(ALL A)
         STDDEV_POP(DISTINCT A)
         STDDEV_SAMP(A)
         STDDEV_SAMP(ALL A)
         STDDEV_SAMP(DISTINCT A)
         SUM(A)
         SUM(ALL A)
         SUM(DISTINCT A)
         VAR_POP(A)
         VAR_POP(ALL A)
         VAR_POP(DISTINCT A)
         VAR_SAMP(A)
         VAR_SAMP(ALL A)
         VAR_SAMP(DISTINCT A)
         VARIANCE(A)
         VARIANCE(ALL A)
         VARIANCE(DISTINCT A)
         */
    }

    @Test
    public void testOrderBy() throws SQLException {
        String query = "SELECT CODE, SALARY from sample_07 ORDER BY SALARY";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "35-3021", 16700L);
        matchSingleRowExplain(query, "SELECT `CODE`, `SALARY` FROM `default`.`SAMPLE_07` ORDER BY `SALARY`");
    }

    @Test
    public void testOrderByLimit() throws SQLException {
        String query = "SELECT CODE, SALARY from sample_07 ORDER BY SALARY LIMIT 3";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "35-3021", 16700L);
        matchNextRow(result, "35-2011", 16860L);
        matchLastRow(result, "35-9021", 17060L);
        matchSingleRowExplain(query, "SELECT `CODE`, `SALARY` FROM `default`.`SAMPLE_07` ORDER BY `SALARY` LIMIT 3");
    }

    @Test
    public void testOrderByLimitOffset() throws SQLException {
        String query = "SELECT CODE, SALARY from sample_07 ORDER BY SALARY LIMIT 2 OFFSET 1";
        ResultSet result = executeQuery(query);
        matchNextRow(result, "35-2011", 16860L);
        matchLastRow(result, "35-9021", 17060L);
        matchSingleRowExplain(query, "SELECT `CODE`, `SALARY` FROM `default`.`SAMPLE_07` ORDER BY `SALARY` LIMIT 2 OFFSET 1");
    }

    @Test
    public void testAggregateFunctions() throws SQLException, ClassNotFoundException, FileNotFoundException {
        String query = "SELECT count(*), count(salary), count(distinct salary) FROM sample_07";
        ResultSet result = executeQuery(query);
        matchLastRow(result, new BigDecimal(823), new BigDecimal(819), new BigDecimal(759));
        matchSingleRowExplain(query, "SELECT COUNT(*), COUNT(`SALARY`), COUNT(DISTINCT `SALARY`) FROM `default`.`SAMPLE_07`");
    }

    private static void createImpalaJDBCAdapter() throws SQLException, FileNotFoundException {
        List<String> impalaIncludes = new ArrayList<>();
        impalaIncludes.add(getConfig().getJdbcAdapterPath());
        String jdbcPrefixPath = getConfig().getImpalaJdbcPrefixPath();
        for (String jar : getConfig().getImpalaJdbcJars()) {
            impalaIncludes.add(jdbcPrefixPath + jar);
        }
        createJDBCAdapter(impalaIncludes);
    }

}
