package com.exasol.adapter.dialects.teradata;

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
 * Integration test for the Teradata SQL dialect
 */
@Tag("integration")
@ExtendWith(IntegrationTestConfigurationCondition.class)
class TeradataSqlDialectIT extends AbstractIntegrationTest {
//    private static final String VIRTUAL_SCHEMA = "VS_TERADATA";
//    private static final String TERADATA_SCHEMA = "retail";
//
//    @BeforeAll
//    static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
//        Assume.assumeTrue(getConfig().teradataTestsRequested());
//        setConnection(connectToExa());
//
//        createTeradataJDBCAdapter();
//        createVirtualSchema(VIRTUAL_SCHEMA, TeradataSqlDialect.NAME, "", TERADATA_SCHEMA, "",
//                getConfig().getTeradataUser(), getConfig().getTeradataPassword(), "ADAPTER.JDBC_ADAPTER",
//                getConfig().getTeradataJdbcConnectionString(), false, getConfig().debugAddress(),
//                "numeric_data_types, REGION, DateTime_and_Interval_Data_Types, Period_Data_Types", null, "");
//    }
//
//    @Test
//    void testSelectNumericDataTypes() throws SQLException, ClassNotFoundException, FileNotFoundException {
//        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA + ".\"numeric_data_types\"";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, (short) 1, 2, (long) 3, new BigDecimal(4), new BigDecimal("7.22"), 1214325.1234,
//                1.3451345135541E9, 1.234513245783E9, 113.321, 3143.0, 2.3452345E7, new BigDecimal("1234.1"),
//                (short) 132);
//        matchSingleRowExplain(query,
//                "SELECT \"mybyteint\", \"mysmallint\", \"myinteger\", \"myBIGINT\", \"myDecimal\", \"myFloat\", \"myReal\", \"myDouble\", CAST(\"n1\"  as DOUBLE PRECISION), CAST(\"n2\"  as DOUBLE PRECISION), CAST(\"n3\"  as DOUBLE PRECISION), \"n4\", \"n5\" FROM \"retail\".\"numeric_data_types\"");
//    }
//
//    @Test
//    void testSelectDateTime_and_Interval_Data_Types()
//            throws SQLException, ClassNotFoundException, FileNotFoundException {
//        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA + ".\"DateTime_and_Interval_Data_Types\"";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, getSqlDate(2017, 1, 11), "13:09:52.000000", getSqlTimestamp(2017, 01, 11, 13, 9, 52, 430),
//                "13:09:52.000000+00:00", getSqlTimestamp(2017, 01, 11, 13, 9, 52, 430),
//                " -2                           ", " 10-10                        ", "   30 12:30:30.5000           ",
//                " 6:15.24                      ");
//        matchSingleRowExplain(query,
//                "SELECT \"myDate\", CAST(\"myTime\"  as VARCHAR(21) ), \"myTimestamp\", CAST(\"myTimeWithTimezone\"  as VARCHAR(21) ), \"myTimestampWithTimezone\", CAST(\"myIntervalYear\"  as VARCHAR(30) ), CAST(\"myIntervalYearToMonth\"  as VARCHAR(30) ), CAST(\"myIntervalDayToSecond\"  as VARCHAR(30) ), CAST(\"myIntervalMinuteToSecond\"  as VARCHAR(30) ) FROM \"retail\".\"DateTime_and_Interval_Data_Types\"");
//    }
//
//    @Test
//    void testSelectPeriod_Data_Types() throws SQLException, ClassNotFoundException, FileNotFoundException {
//        final String query = "SELECT * FROM  " + VIRTUAL_SCHEMA + ".\"Period_Data_Types\"";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, (long) 1, "hans           ", "('05/02/03', '06/02/04')",
//                "('10:00:00.123456', '11:00:00.123456')", "('10:37:58.123456+08:00', '11:37:58.123456+08:00')",
//                "('2005-02-03 10:00:00.123', '2005-02-03 11:00:00.123')",
//                "('2005-02-03 10:37:58.123+08:00', '2005-02-03 11:37:58.123+08:00')"
//
//        );
//        matchSingleRowExplain(query,
//                "SELECT \"employee_id\", \"employee_name\", CAST(\"myPeriodDate\"  as VARCHAR(100) ), CAST(\"myPeriodTime\"  as VARCHAR(100) ), CAST(\"myPeriodTimeWithTimeZone\"  as VARCHAR(100) ), CAST(\"myPeriodTimestamp\"  as VARCHAR(100) ), CAST(\"myPeriodTimestampTimezone\"  as VARCHAR(100) ) FROM \"retail\".\"Period_Data_Types\"");
//    }
//
//    @Test
//    void testProjection() throws SQLException, ClassNotFoundException, FileNotFoundException {
//        final String query = "SELECT R_REGIONKEY FROM  " + VIRTUAL_SCHEMA + ".REGION order by R_REGIONKEY";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, (long) 0);
//        matchSingleRowExplain(query, "SELECT R_REGIONKEY FROM \"retail\".REGION ORDER BY R_REGIONKEY");
//    }
//
//    private static void createTeradataJDBCAdapter() throws SQLException, FileNotFoundException {
//        final List<String> teradataIncludes = new ArrayList<>();
//        teradataIncludes.add(getConfig().getJdbcAdapterPath());
//        final String jdbcPrefixPath = getConfig().getTeradataJdbcPrefixPath();
//        for (final String jar : getConfig().getTeradataJdbcJars()) {
//            teradataIncludes.add(jdbcPrefixPath + jar);
//        }
//        createJDBCAdapter(teradataIncludes);
//    }
}
