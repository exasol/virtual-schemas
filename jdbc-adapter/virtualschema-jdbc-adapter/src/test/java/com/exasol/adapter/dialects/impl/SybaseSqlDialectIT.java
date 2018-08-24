package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SybaseSqlDialectIT extends AbstractIntegrationTest {

    private static final boolean IS_LOCAL = false;

    @BeforeClass
    public static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().sybaseTestsRequested());

        setConnection(connectToExa());
        createSybaseJDBCAdapter();
        String catalogName = "testdb";       // This only works for the database in our test environment
        String schemaName = "tester";
        createVirtualSchema("VS_SYBASE",
                SybaseSqlDialect.NAME,
                catalogName,
                schemaName,
                "",
                getConfig().getSybaseUser(),
                getConfig().getSybasePassword(),
                "ADAPTER.JDBC_ADAPTER",
                getConfig().getSybaseJdbcConnectionString(),
                IS_LOCAL,
                getConfig().debugAddress(),
                "", null);
    }

    private static void createSybaseJDBCAdapter() throws SQLException, FileNotFoundException {
        String jdbcAdapterPath = getConfig().getJdbcAdapterPath();
        String jdbcDriverDriver = getConfig().getSybaseJdbcDriverPath();
        List<String> includes = new ArrayList<>();
        includes.add(jdbcAdapterPath);
        includes.add(jdbcDriverDriver);
        createJDBCAdapter(includes);
    }

    @Test
    public void testSelect() throws SQLException {
        ResultSet result = executeQuery("SELECT * FROM vs_sybase.\"ittable\"");
        matchNextRow(result, "e", 2L);
    }

    @Test
    public void testProjection() throws SQLException {
        ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\"");
        matchNextRow(result, "e");
    }

    @Test
    public void testOrderByAsc() throws SQLException {
        ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\"");
        matchNextRow(result, "a");
        result.last();
        assertEquals(null, result.getObject(1));
    }

    @Test
    public void testOrderByAscNullsFirst() throws SQLException {
        ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" NULLS FIRST");
        result.next();
        assertEquals(null, result.getObject(1));
        result.last();
        matchLastRow(result, "z");
    }

    @Test
    public void testOrderByDesc() throws SQLException {
        ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" DESC");
        result.next();
        assertEquals(null, result.getObject(1));
        result.last();
        matchLastRow(result, "a");
    }

    @Test
    public void testOrderByDescNullsLast() throws SQLException {
        ResultSet result = executeQuery("SELECT \"a\" FROM vs_sybase.\"ittable\" ORDER BY \"a\" DESC NULLS LAST");
        matchNextRow(result, "z");
        result.last();
        assertEquals(null, result.getObject(1));
    }

    @Test
    public void testWhereGreater() throws SQLException {
        ResultSet result = executeQuery("SELECT \"b\" FROM vs_sybase.\"ittable\" WHERE \"b\" > 0");
        result.last();
        assertEquals(2, result.getRow());
    }
}
