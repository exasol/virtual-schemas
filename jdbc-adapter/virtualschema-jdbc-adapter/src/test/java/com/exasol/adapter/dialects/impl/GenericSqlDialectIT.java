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

public class GenericSqlDialectIT extends AbstractIntegrationTest {

    private static final boolean IS_LOCAL = false;


    @BeforeClass
    public static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().genericTestsRequested());

        String connectionString = getConfig().getGenericJdbcConnectionString();
        setConnection(connectToExa());
        createGenericJDBCAdapter();
        String catalogName = "jm3450";       // This only works for the database in our test environment
        String schemaName = "";
        createVirtualSchema("VS_GENERIC_MYSQL",
                GenericSqlDialect.NAME,
                catalogName,
                schemaName,
                "",
                getConfig().getGenericUser(),
                getConfig().getGenericPassword(),
                "ADAPTER.JDBC_ADAPTER",
                connectionString,
                IS_LOCAL,
                getConfig().debugAddress(),
                "", null);
    }

    @Test
    public void testVirtualSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        ResultSet result = executeQuery("SELECT * FROM \"customers\" ORDER BY id");
        result.next();
        assertEquals("1", result.getString(1));
    }

    private static void createGenericJDBCAdapter() throws SQLException, FileNotFoundException {
        String jdbcAdapterPath = getConfig().getJdbcAdapterPath();
        String jdbcDriverDriver = getConfig().getGenericJdbcDriverPath();
        List<String> includes = new ArrayList<>();
        includes.add(jdbcAdapterPath);
        includes.add(jdbcDriverDriver);
        createJDBCAdapter(includes);
    }
}
