package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import com.exasol.adapter.dialects.SqlDialects;
import com.exasol.adapter.jdbc.JdbcMetadataReader;
import com.exasol.adapter.json.SchemaMetadataSerializer;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.google.common.collect.ImmutableList;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNotNull;

/**
 * Integration tests for the EXASOL SQL dialect.
 */
public class ExasolSqlDialectIT extends AbstractIntegrationTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String testSchema = "NATIVE_EXA_IT";
    private static final String testSchemaMixedCase = "NATIVE_EXA_IT_Mixed_Case";
    private static final String virtualSchema = "VS_EXA_IT";
    private static final String virtualSchemaMixedCase = "VS_EXA_IT_MIXED_CASE";

    @BeforeClass
    public static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().exasolTestsRequested());
        setConnection(connectToExa());
        String connectionString = "jdbc:exa:localhost:" + getPortOfConnectedDatabase();  // connect via Virtual Schema to local database
        // The EXASOL jdbc driver is included in the Maven dependencies, so no need to add
        List<String> includes = ImmutableList.of(getConfig().getJdbcAdapterPath());
        createJDBCAdapter(includes);
        createTestSchema();
        createVirtualSchema(
                virtualSchema,
                ExasolSqlDialect.NAME,
                "", testSchema,
                "",
                getConfig().getExasolUser(),
                getConfig().getExasolPassword(),
                "ADAPTER.JDBC_ADAPTER",
                connectionString, true,
                getConfig().debugAddress(),
                "");
        createVirtualSchema(
                virtualSchemaMixedCase,
                ExasolSqlDialect.NAME,
                "", testSchemaMixedCase,
                "",
                getConfig().getExasolUser(),
                getConfig().getExasolPassword(),
                "ADAPTER.JDBC_ADAPTER",
                connectionString, true,
                getConfig().debugAddress(),
                "");
    }

    private static void createTestSchema() throws SQLException {
        // EXASOL integration test is special, because we can directly create our test data.
        // For other dialects you have to prepare the source data base separately, because
        // otherwise we would need to make the jdbc driver visible to the integration test framework as well (adds complexity)
        Statement stmt = getConnection().createStatement();
        stmt.execute("DROP SCHEMA IF EXISTS " + testSchema + " CASCADE");
        stmt.execute("CREATE SCHEMA " + testSchema);
        stmt.execute("CREATE TABLE ALL_EXA_TYPES (" +
                        " c1 varchar(100) default 'bar'," +
                        " c2 varchar(100) CHARACTER SET ASCII default 'bar'," +
                        " c3 char(10) default 'foo'," +
                        " c4 char(10) CHARACTER SET ASCII default 'bar'," +
                        " c5 decimal(5,0) default 1," +
                        " c6 decimal(6,3) default 1.2," +
                        " c7 double default 1E2," +
                        " c8 boolean default TRUE," +
                        " c9 date default '2016-06-01'," +
                        " c10 timestamp default '2016-06-01 00:00:01.000'," +
                        " c11 timestamp with local time zone default '2016-06-01 00:00:02.000'," +
                        " c12 interval year to month default '3-5'," +
                        " c13 interval day to second default '2 12:50:10.123'," +
                        " c14 geometry(3857) default 'POINT(2 5)'" +
                        ")");

        stmt.execute("INSERT INTO " + testSchema + ".ALL_EXA_TYPES VALUES(" +
                "'a茶'," +
                "'b'," +
                "'c茶'," +
                "'d'," +
                "123," +
                "123.456," +
                "2.2," +
                "FALSE," +
                "'2016-08-01'," +
                "'2016-08-01 00:00:01.000'," +
                "'2016-08-01 00:00:02.000'," +
                "'4-6'," +
                "'3 12:50:10.123'," +
                "'POINT(2 5)'" +
                ");");

        stmt.execute("CREATE TABLE WITH_NULLS (c1 int, c2 varchar(100))");
        stmt.execute("INSERT INTO WITH_NULLS VALUES " +
                " (1, 'a')," +
                " (2, null)," +
                " (3, 'b')," +
                " (1, null)," +
                " (null, 'c')");

        // Create schema, table and column with mixed case identifiers (to test correct mapping, and correct sql generation of adapter)
        stmt.execute("DROP SCHEMA IF EXISTS \"" + testSchemaMixedCase + "\" CASCADE");
        stmt.execute("CREATE SCHEMA \"" + testSchemaMixedCase + "\"");
        stmt.execute("CREATE TABLE \"Table_Mixed_Case\" (\"Column1\" int, \"column2\" int, COLUMN3 int)");
        stmt.execute("INSERT INTO \"Table_Mixed_Case\" VALUES (1, 2, 3)");
    }

    @Test
    public void testDataTypeMapping() throws SQLException {
        ResultSet result = executeQuery("SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, COLUMN_DEFAULT FROM EXA_DBA_COLUMNS WHERE COLUMN_SCHEMA = '" + virtualSchema + "' AND COLUMN_TABLE='ALL_EXA_TYPES' ORDER BY COLUMN_ORDINAL_POSITION");
        matchNextRow(result, "C1", "VARCHAR(100) UTF8", (long)100, null, null, "'bar'");
        matchNextRow(result, "C2", "VARCHAR(100) ASCII", (long)100, null, null, "'bar'");
        matchNextRow(result, "C3", "CHAR(10) UTF8", (long)10, null, null, "'foo'");
        matchNextRow(result, "C4", "CHAR(10) ASCII", (long)10, null, null, "'bar'");
        matchNextRow(result, "C5", "DECIMAL(5,0)", (long)5, (long)5, (long)0, "1");
        matchNextRow(result, "C6", "DECIMAL(6,3)", (long)6, (long)6, (long)3, "1.2");
        matchNextRow(result, "C7", "DOUBLE", (long)64, null, null, "100");
        matchNextRow(result, "C8", "BOOLEAN", (long)1, null, null, "TRUE");
        matchNextRow(result, "C9", "DATE", (long)10, null, null, "'2016-06-01'");
        matchNextRow(result, "C10", "TIMESTAMP", (long)29, null, null, "'2016-06-01 00:00:01.000'");
        matchNextRow(result, "C11", "TIMESTAMP WITH LOCAL TIME ZONE", (long)29, null, null, "'2016-06-01 00:00:02.000'");
        matchNextRow(result, "C12", "INTERVAL YEAR(2) TO MONTH", (long)13, null, null, "'3-5'");
        matchNextRow(result, "C13", "INTERVAL DAY(2) TO SECOND(3)", (long)29, null, null, "'2 12:50:10.123'");
        matchLastRow(result, "C14", "GEOMETRY(3857)", (long)8000000, null, null, "'POINT(2 5)'");       // srid not yet supported, so will always default to 3857
    }

    @Test
    public void testDataTypeSelect() throws SQLException {
        ResultSet result = executeQuery("SELECT * FROM " + virtualSchema + ".ALL_EXA_TYPES");
        matchNextRow(result,
                "a茶",
                "b",
                "c茶        ",
                "d         ",
                123,
                new BigDecimal("123.456"),
                2.2,
                false,
                getSqlDate(2016,8,1),
                getSqlTimestamp(2016,8,1,0,0,1,0),
                getSqlTimestamp(2016,8,1,0,0,2,0),
                "+04-06",
                "+03 12:50:10.123",
                "POINT (2 5)");
    }

    @Test
    public void testIdentifierCaseSensitivity() throws SQLException, FileNotFoundException {
        ResultSet result = executeQuery("SELECT * FROM \"Table_Mixed_Case\"");
        matchLastRow(result, 1L, 2L, 3L);
        result = executeQuery("SELECT \"Column1\", \"column2\", COLUMN3 FROM \"Table_Mixed_Case\"");
        matchLastRow(result, 1L, 2L, 3L);
        result = executeQuery("SELECT \"Column1\", \"column2\", COLUMN3 FROM \"Table_Mixed_Case\"");
        matchLastRow(result, 1L, 2L, 3L);
    }

    @Test
    public void testIdentifierCaseSensitivityException1() throws SQLException, FileNotFoundException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("object TABLE_MIXED_CASE not found");
        executeQuery("SELECT \"Column1\", \"column2\", COLUMN3 FROM Table_Mixed_Case");
    }

    @Test
    public void testIdentifierCaseSensitivityException2() throws SQLException, FileNotFoundException {
        thrown.expect(SQLException.class);
        thrown.expectMessage("object COLUMN1 not found");
        executeQuery("SELECT Column1, column2, COLUMN3 FROM \"Table_Mixed_Case\"");
    }

    /**
     * This was replaced by integration test {@link #testDataTypeMapping()}. It can be enabled temporarily for debugging.
     */
    @Ignore
    @Test
    public void testDifferentDataTypes() throws SQLException, ClassNotFoundException, FileNotFoundException {
        Statement stmt = getConnection().createStatement();
        String jdbc_adapter_test_schema = "JDBC_ADAPTER_TEST_SCHEMA";
        String sql = "DROP SCHEMA IF EXISTS " + jdbc_adapter_test_schema + " CASCADE";
        stmt.execute(sql);
        sql = "CREATE SCHEMA " + jdbc_adapter_test_schema;
        stmt.execute(sql);
        sql = "CREATE TABLE T8(c1 boolean default TRUE, c2 char(10) default 'foo'" +
                ", c3 date default '2016-06-01', c4 decimal(5,0) default 0)";
        stmt.execute(sql);
        sql = "CREATE TABLE T9(c1 double default 1E2, c2 geometry default 'POINT(2 5)'" +
                ", c3 interval year to month default '3-5', c4 interval day to second default '2 12:50:10.123')";
        stmt.execute(sql);
        sql = "CREATE TABLE TA(c1 timestamp default '2016-06-01 00:00:01.000'" +
                ", c2 timestamp with local time zone default '2016-06-01 00:00:02.000', c3 varchar(100) default 'bar')";
        stmt.execute(sql);
        sql = "CREATE TABLE TB(c1 boolean default NULL, c2 char(10) default NULL" +
                ", c3 date default NULL, c4 decimal(5,0) default NULL)";
        stmt.execute(sql);
        sql = "CREATE TABLE TC(c1 double default NULL, c2 geometry default NULL" +
                ", c3 interval year to month default NULL, c4 interval day to second default NULL)";
        stmt.execute(sql);
        sql = "CREATE TABLE TD(c1 timestamp default NULL, c2 timestamp with local time zone default NULL" +
                ", c3 varchar(100) default NULL)";
        stmt.execute(sql);
        String[] tableNames = new String[]{"T8", "T9", "TA", "TB", "TC", "TD"};
        List<String> tables = new ArrayList<>(Arrays.asList(tableNames));
        SqlDialects dialects = new SqlDialects(ImmutableList.of(ExasolSqlDialect.NAME));
        SchemaMetadata meta = JdbcMetadataReader.readRemoteMetadata("jdbc:exa:" + getConfig().getExasolAddress(),
                getConfig().getExasolUser(), getConfig().getExasolPassword(), "EXA_DB", "JDBC_ADAPTER_TEST_SCHEMA", tables, dialects, ExasolSqlDialect.NAME);
        if (getConfig().isDebugOn()) {
            System.out.println("Meta: " + SchemaMetadataSerializer.serialize(meta).build().toString());
        }
        assertNotNull(meta);
    }

}
