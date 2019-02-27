package com.exasol.adapter.dialects.impl;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.exasol.adapter.AdapterException;
import com.exasol.jdbc.DataException;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import org.junit.rules.ExpectedException;

/**
 * Integration test for the PostgreSQL SQL dialect
 *
 */
public class PostgreSQLDialectIT extends AbstractIntegrationTest {

    private static final String VIRTUAL_SCHEMA = "PG";
    private static final String VIRTUAL_SCHEMA_UPPERCASE_TABLE = "PG_UPPER";
    private static final String POSTGRES_CATALOG = "postgres";
    private static final String POSTGRES_SCHEMA = "public";
    private static final String POSTGRES_SCHEMA_UPPERCASE_TABLE = "upper";
    private static final boolean IS_LOCAL = false;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().getPostgresqlTestsRequested());
        setConnection(connectToExa());

        createTestSchema();
        createTestSchemaUpperCaseTable();
        createPostgreSQLJDBCAdapter();
        createVirtualSchema(VIRTUAL_SCHEMA, PostgreSQLSqlDialect.getPublicName(), POSTGRES_CATALOG, POSTGRES_SCHEMA, "", getConfig().getPostgresqlUser(),
                getConfig().getPostgresqlPassword(), "ADAPTER.JDBC_ADAPTER", getConfig().getPostgresqlDockerJdbcConnectionString(),
                IS_LOCAL, getConfig().debugAddress(), "", null, "");
        createVirtualSchema(VIRTUAL_SCHEMA_UPPERCASE_TABLE, PostgreSQLSqlDialect.getPublicName(), POSTGRES_CATALOG, POSTGRES_SCHEMA_UPPERCASE_TABLE, "",
                getConfig().getPostgresqlUser(), getConfig().getPostgresqlPassword(), "ADAPTER.JDBC_ADAPTER",
                getConfig().getPostgresqlDockerJdbcConnectionString(), IS_LOCAL, getConfig().debugAddress(), "",
                "ignore_errors='POSTGRESQL_UPPERCASE_TABLES'", "");
    }

    private static void createTestSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String user = getConfig().getPostgresqlUser();
        final String password = getConfig().getPostgresqlPassword();
        final String postgresConnectionString = getConfig().getPostgresqlJdbcConnectionString();
        Class.forName("org.postgresql.Driver");
        try (Connection conn = DriverManager.getConnection(postgresConnectionString, user, password))
        {
            final Statement stmt = conn.createStatement();
            stmt.execute(String.format("create table %s.t(x int)", POSTGRES_SCHEMA));
            stmt.execute(String.format("insert into %s.t values (1);", POSTGRES_SCHEMA));
        }
    }

    private static void createTestSchemaUpperCaseTable() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String user = getConfig().getPostgresqlUser();
        final String password = getConfig().getPostgresqlPassword();
        final String postgresConnectionString = getConfig().getPostgresqlJdbcConnectionString();
        Class.forName("org.postgresql.Driver");
        try (Connection conn = DriverManager.getConnection(postgresConnectionString, user, password))
        {
            final Statement stmt = conn.createStatement();
            stmt.execute(String.format("create schema %s", POSTGRES_SCHEMA_UPPERCASE_TABLE));
            stmt.execute(String.format("create table %s.lower_t(x int, y int)", POSTGRES_SCHEMA_UPPERCASE_TABLE));
            stmt.execute(String.format("create table %s.\"UPPer_t\"(x int, y int)", POSTGRES_SCHEMA_UPPERCASE_TABLE));
        }
    }

    @Test
    public void testOpenVirtualSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String open_schema_query = String.format("OPEN SCHEMA %s", VIRTUAL_SCHEMA);
        execute(open_schema_query);
    }

    @Test
    public void testSelectSingleColumn() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String query = String.format("SELECT x FROM %s.t", VIRTUAL_SCHEMA);
        final ResultSet result = executeQuery(query);
        matchNextRow(result, 1L);
    }

    @Test
    public void testCreateSchemaWithUpperCaseTables() throws SQLException, ClassNotFoundException, FileNotFoundException {
        expectedEx.expect(DataException.class);
        expectedEx.expectMessage("Table UPPer_t cannot be used in virtual schema. Set property IGNORE_ERRORS to POSTGRESQL_UPPERCASE_TABLES to enforce schema creation.");
        createVirtualSchema("FOO", PostgreSQLSqlDialect.getPublicName(), POSTGRES_CATALOG, POSTGRES_SCHEMA_UPPERCASE_TABLE, "",
                getConfig().getPostgresqlUser(), getConfig().getPostgresqlPassword(), "ADAPTER.JDBC_ADAPTER",
                getConfig().getPostgresqlDockerJdbcConnectionString(), IS_LOCAL, getConfig().debugAddress(), "", null, "JOIN");
    }

    @Test
    public void testQueryUpperCaseTableQuoted() throws SQLException, ClassNotFoundException, FileNotFoundException {
        expectedEx.expect(DataException.class);
        expectedEx.expectMessage("Cannot resolve column types");
        final String query = String.format("SELECT x FROM  %s.\"UPPer_t\"", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        final ResultSet result = executeQuery(query);
    }

    @Test
    public void testQueryUpperCaseTable() throws SQLException, ClassNotFoundException, FileNotFoundException {
        expectedEx.expect(SQLException.class);
        expectedEx.expectMessage("object PG_UPPER.UPPER_T not found");
        final String query = String.format("SELECT x FROM  %s.UPPer_t", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        final ResultSet result = executeQuery(query);
    }

    @Test
    public void testQueryLowerCaseTable() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String query = String.format("SELECT x FROM  %s.lower_t", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        final ResultSet result = executeQuery(query);
    }

    @Test
    public void testUnsetIgnoreUpperCaseTablesAndRefresh() throws SQLException, ClassNotFoundException, FileNotFoundException {
        expectedEx.expect(DataException.class);
        expectedEx.expectMessage("Table UPPer_t cannot be used in virtual schema. Set property IGNORE_ERRORS to POSTGRESQL_UPPERCASE_TABLES to enforce schema creation.");
        final String alter_schema_query = String.format("ALTER VIRTUAL SCHEMA %s set ignore_errors=''", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(alter_schema_query);
        final String refresh_schema_query = String.format("ALTER VIRTUAL SCHEMA %s REFRESH", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(refresh_schema_query);
    }

    @Test
    public void testSetIgnoreUpperCaseTablesAndRefresh() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String alter_schema_query = String.format("ALTER VIRTUAL SCHEMA %s set ignore_errors='POSTGRESQL_UPPERCASE_TABLES'", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(alter_schema_query);
        final String refresh_schema_query = String.format("ALTER VIRTUAL SCHEMA %s REFRESH", VIRTUAL_SCHEMA_UPPERCASE_TABLE);
        execute(refresh_schema_query);
    }

    private static void createPostgreSQLJDBCAdapter() throws SQLException, FileNotFoundException {
        final List<String> PostgreSQLIncludes = new ArrayList<>();
        PostgreSQLIncludes.add(getConfig().getJdbcAdapterPath());
        final String jdbcDriverPath = getConfig().getPostgresqlJdbcDriverPath();
        PostgreSQLIncludes.add(jdbcDriverPath);
        createJDBCAdapter(PostgreSQLIncludes);
    }



}