package utils;

import java.sql.SQLException;
import java.sql.Statement;

public class IntegrationTestSetupManager {
    public void createTestSchema(final Statement statement, final String schemaName) throws SQLException {
        statement.execute("CREATE SCHEMA " + schemaName);
    }
}
