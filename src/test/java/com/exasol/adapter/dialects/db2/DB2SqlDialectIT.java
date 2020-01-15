package com.exasol.adapter.dialects.db2;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assume;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import com.exasol.adapter.dialects.IntegrationTestConfigurationCondition;

/**
 * Integration test for theDB2 SQL dialect
 */
@ExtendWith(IntegrationTestConfigurationCondition.class)
class DB2SqlDialectIT extends AbstractIntegrationTest {
//    private static final String VIRTUAL_SCHEMA = "DB2";
//    private static final String DB2_SCHEMA = "DB2TEST";
//    private static final boolean IS_LOCAL = false;
//
//    @BeforeAll
//    static void beforeAll() throws FileNotFoundException, SQLException, ClassNotFoundException {
//        Assume.assumeTrue(getConfig().DB2TestsRequested());
//        setConnection(connectToExa());
//
//        createDB2JDBCAdapter();
//        createVirtualSchema(VIRTUAL_SCHEMA, DB2SqlDialect.getPublicName(), "", DB2_SCHEMA, "", getConfig().getDB2User(),
//                getConfig().getDB2Password(), "ADAPTER.JDBC_ADAPTER", getConfig().getDB2JdbcConnectionString(),
//                IS_LOCAL, getConfig().debugAddress(), "", null, "");
//    }
//
//    @Test
//    void testSelectNumericDataTypes() throws SQLException {
//        final String query = "SELECT PRICE,PROMOPRICE FROM " + VIRTUAL_SCHEMA + ".PRODUCT WHERE pid = '100-100-01'";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, new BigDecimal("9.99"), new BigDecimal("7.25"));
//        matchSingleRowExplain(query, "SELECT \"PRICE\", \"PROMOPRICE\" FROM \"" + DB2_SCHEMA
//                + "\".\"PRODUCT\" WHERE \"PID\" = '100-100-01'");
//    }
//
//    @Test
//    void testLimit() throws SQLException {
//        final String query = "SELECT * FROM (SELECT price,PROMOPRICE FROM DB2.PRODUCT) AS A LIMIT 1 ";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, new BigDecimal("9.99"), new BigDecimal("7.25"));
//        matchSingleRowExplain(query,
//                "SELECT \"PRICE\", \"PROMOPRICE\" FROM \"" + DB2_SCHEMA + "\".\"PRODUCT\" FETCH FIRST 1 ROWS ONLY");
//    }
//
//    @Test
//    void testTimeDataTypeConversions() throws SQLException {
//        final String query = "SELECT DETAIL_TIMESTAMP,UHRZEIT FROM " + VIRTUAL_SCHEMA
//                + ".\"Additional_Datatypes\"  WHERE DETAIL_TIMESTAMP = '2020-01-01-00.00.00.123456789123'";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, "2020-01-01-00.00.00.123456789123", "12.05.11");
//        matchSingleRowExplain(query, "SELECT VARCHAR(\"DETAIL_TIMESTAMP\"), VARCHAR(\"UHRZEIT\") FROM \"" + DB2_SCHEMA
//                + "\".\"Additional_Datatypes\" WHERE \"DETAIL_TIMESTAMP\" = '2020-01-01-00.00.00.123456789123'");
//
//    }
//
//    @Test
//    void testBitDataConversion() throws SQLException {
//        final String query = "SELECT BIDATAVARCHAR,BIDATACHAR FROM " + VIRTUAL_SCHEMA
//                + ".\"Additional_Datatypes\"  WHERE DETAIL_TIMESTAMP = '2020-01-01-00.00.00.123456789123'";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, "30303031",
//                "41414242202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020");
//        matchSingleRowExplain(query, "SELECT HEX(\"BIDATAVARCHAR\"), HEX(\"BIDATACHAR\") FROM \"" + DB2_SCHEMA
//                + "\".\"Additional_Datatypes\" WHERE \"DETAIL_TIMESTAMP\" = '2020-01-01-00.00.00.123456789123'");
//
//    }
//
//    @Test
//    void testUnicode() throws SQLException {
//        final String query = "SELECT UNICODECOL FROM " + VIRTUAL_SCHEMA
//                + ".\"Additional_Datatypes\"  WHERE DETAIL_TIMESTAMP = '2020-01-01-00.00.00.123456789123'";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, "CHAR 茶");
//        matchSingleRowExplain(query, "SELECT \"UNICODECOL\" FROM \"" + DB2_SCHEMA
//                + "\".\"Additional_Datatypes\" WHERE \"DETAIL_TIMESTAMP\" = '2020-01-01-00.00.00.123456789123'");
//
//    }
//
//    @Test
//    void testScalarFunctions() throws SQLException {
//        final String query = "SELECT ADD_DAYS(DETAIL_TIMESTAMP,2),ADD_YEARS(DETAIL_TIMESTAMP,-2),SUBSTR(UNICODECOL,1,4) FROM "
//                + VIRTUAL_SCHEMA
//                + ".\"Additional_Datatypes\"  WHERE DETAIL_TIMESTAMP = '2020-01-01-00.00.00.123456789123'";
//        final ResultSet result = executeQuery(query);
//        assertNextRow(result, "2020-01-03-00.00.00.123456789123", "2018-01-01-00.00.00.123456789123", "CHAR");
//        matchSingleRowExplain(query,
//                "SELECT VARCHAR(\"DETAIL_TIMESTAMP\" + 2 DAYS), VARCHAR(\"DETAIL_TIMESTAMP\" + -2 YEARS), SUBSTR(\"UNICODECOL\", 1, 4) FROM \""
//                        + DB2_SCHEMA
//                        + "\".\"Additional_Datatypes\" WHERE \"DETAIL_TIMESTAMP\" = '2020-01-01-00.00.00.123456789123'");
//    }
//
//    private static void createDB2JDBCAdapter() throws SQLException, FileNotFoundException {
//        final List<String> DB2Includes = new ArrayList<>();
//        DB2Includes.add(getConfig().getJdbcAdapterPath());
//        final String jdbcPrefixPath = getConfig().getDB2JdbcPrefixPath();
//        for (final String jar : getConfig().getDB2JdbcJars()) {
//            DB2Includes.add(jdbcPrefixPath + jar);
//        }
//        createJDBCAdapter(DB2Includes);
//    }
}
