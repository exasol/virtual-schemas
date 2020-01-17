package utils;

import java.sql.SQLException;
import java.sql.Statement;
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
}
