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

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import com.exasol.adapter.dialects.AbstractIntegrationTest;

/**
 * Integration test for the PostgreSQL SQL dialect
 *
 */
public class PostgreSQLDialectIT extends AbstractIntegrationTest {

    private static final String VIRTUAL_SCHEMA = "PG";
    private static final String POSTGRES_CATALOG = "postgres";
    private static final String POSTRGES_SCHEMA = "public";
    private static final boolean IS_LOCAL = false;

    @BeforeClass
    public static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().getPostgresqlTestsRequested());
        setConnection(connectToExa());

        createTestSchema();
        createPostgreSQLJDBCAdapter();
        createVirtualSchema(VIRTUAL_SCHEMA, PostgreSQLSqlDialect.getPublicName(), POSTGRES_CATALOG, POSTRGES_SCHEMA, "", getConfig().getPostgresqlUser(),
                getConfig().getPostgresqlPassword(), "ADAPTER.JDBC_ADAPTER", getConfig().getPostgresqlDockerJdbcConnectionString(),
                IS_LOCAL, getConfig().debugAddress(), "", null);
    }

    private static void createTestSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String user = getConfig().getPostgresqlUser();
        final String password = getConfig().getPostgresqlPassword();
        final String postgresConnectionString = getConfig().getPostgresqlJdbcConnectionString();
        Class.forName("org.postgresql.Driver");
        try (Connection conn = DriverManager.getConnection(postgresConnectionString, user, password))
        {
            final Statement stmt = conn.createStatement();
            stmt.execute("create table " + POSTRGES_SCHEMA + ".t(x int)");
        }
    }

    @Test
    public void testOpenVirtualSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        final String open_schema_query = "OPEN SCHEMA " + VIRTUAL_SCHEMA;
        execute(open_schema_query);
    }

    private static void createPostgreSQLJDBCAdapter() throws SQLException, FileNotFoundException {
        final List<String> PostgreSQLIncludes = new ArrayList<>();
        PostgreSQLIncludes.add(getConfig().getJdbcAdapterPath());
        final String jdbcDriverPath = getConfig().getPostgresqlJdbcDriverPath();
        PostgreSQLIncludes.add(jdbcDriverPath);
        createJDBCAdapter(PostgreSQLIncludes);
    }



}