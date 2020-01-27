package utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Optional;

public class IntegrationTestSetupManager {
    public void createTestSchema(final Statement statement, final String schemaName) throws SQLException {
        statement.execute("CREATE SCHEMA " + schemaName);
    }

    public void createConnection(final Statement statement, final String connectionName, final String connectionString,
            final String username, final String password) throws SQLException {
        statement.execute("CREATE OR REPLACE CONNECTION " + connectionName + " " //
                + "TO '" + connectionString + "' " //
                + "USER '" + username + "' " //
                + "IDENTIFIED BY '" + password + "'");
    }

    public void createAdapterScript(final Statement statement, final String qualifiedAdapterScriptName,
            final String virtualSchemaJarName, final Optional<String> driver) throws SQLException {
        final StringBuilder builder = new StringBuilder();
        builder.append("CREATE OR REPLACE JAVA ADAPTER SCRIPT ").append(qualifiedAdapterScriptName);
        builder.append(" AS ");
        builder.append("%scriptclass com.exasol.adapter.RequestDispatcher;\n");
        builder.append("%jar /buckets/bfsdefault/default/").append(virtualSchemaJarName).append(";\n");
        driver.ifPresent(builder::append);
        builder.append("/");
        statement.execute(builder.toString());
    }

    public void createTestTablesForJoinTests(final Statement statement, final String schemaName,
            final String firstTableName, final String secondTableName) throws SQLException {
        statement.execute("CREATE TABLE " + schemaName + "." + firstTableName + "(x INT, y VARCHAR(100))");
        statement.execute("INSERT INTO " + schemaName + "." + firstTableName + " VALUES (1,'aaa')");
        statement.execute("INSERT INTO " + schemaName + "." + firstTableName + " VALUES (2,'bbb')");
        statement.execute("CREATE TABLE " + schemaName + "." + secondTableName + "(x INT, y VARCHAR(100))");
        statement.execute("INSERT INTO " + schemaName + "." + secondTableName + " VALUES (2,'bbb')");
        statement.execute("INSERT INTO " + schemaName + "." + secondTableName + " VALUES (3,'ccc')");
    }

    public ResultSet getSelectAllFromJoinExpectedTable(final Statement statement, final String schemaName,
            final String expectedColumns, final String expectedValues) throws SQLException {
        statement.execute("CREATE OR REPLACE TABLE " + schemaName + ".TABLE_JOIN_EXPECTED " + expectedColumns);
        statement.execute("INSERT INTO " + schemaName + ".TABLE_JOIN_EXPECTED " + expectedValues);
        return statement.executeQuery("SELECT * FROM " + schemaName + ".TABLE_JOIN_EXPECTED");
    }

    public Statement getStatement(final JdbcDatabaseContainer container) throws SQLException {
        final DataSource dataSource = getDataSource(container);
        return dataSource.getConnection().createStatement();
    }

    private DataSource getDataSource(final JdbcDatabaseContainer container) {
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());
        return new HikariDataSource(hikariConfig);
    }
}
