package com.exasol.adapter.dialects;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assume;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration test for JDBC drivers requiring Kerberos authentication. This is currently only tested for the Cloudera
 * Hive JDBC driver developed by Simba (probably also works for the Cloudera Impala Driver developed by Simba)
 */
@ExtendWith(IntegrationTestConfigurationCondition.class)
class KerberosIT extends AbstractIntegrationTest {
    private static final String VIRTUAL_SCHEMA = "VS_KERBEROS_IT";
    private static final String CONNECTION_NAME = "krb_conn";
    private static final boolean IS_LOCAL = false;

    @BeforeAll
    static void beforeAll() throws FileNotFoundException, SQLException, ClassNotFoundException {
        Assume.assumeTrue(getConfig().kerberosTestsRequested());
        final String kerberosConnectionString = getConfig().getKerberosJdbcConnectionString();
        final String kerberosUser = getConfig().getKerberosUser();
        final String kerberosPassword = getConfig().getKerberosPassword();
        setConnection(connectToExa());
        createKerberosJDBCAdapter();
        final String connectionName = "krb_conn";
        createConnection(connectionName, kerberosConnectionString, kerberosUser, kerberosPassword);
    }

    @Test
    void testKerberosVirtualSchema() throws SQLException, FileNotFoundException {
        createVirtualSchema(VIRTUAL_SCHEMA, "EXASOL", "", "default", CONNECTION_NAME, "", "", "ADAPTER.JDBC_ADAPTER",
                "", IS_LOCAL, getConfig().debugAddress(), "", null, "");
        final Statement stmt = getConnection().createStatement();
        final ResultSet result = stmt.executeQuery("SELECT * FROM \"sample_07\"");
        result.next();
        assertEquals("00-0000", result.getString(1));
    }

    @Test
    void testKerberosVirtualSchemaGrantConnection() throws SQLException, ClassNotFoundException, FileNotFoundException {
        // Create Kerberos Virtual Schema using a different user, which has the
        // appropriate privileges for the connection
        final String userName = "user1";
        final Statement stmt = getConnection().createStatement();
        stmt.execute("DROP USER IF EXISTS " + userName + " CASCADE");
        stmt.execute("CREATE USER " + userName + " identified by \"" + userName + "\"");
        stmt.execute("GRANT CREATE SESSION TO " + userName);
        stmt.execute("GRANT CREATE VIRTUAL SCHEMA TO " + userName);
        stmt.execute("GRANT DROP ANY VIRTUAL SCHEMA TO " + userName);
        final String adapterName = "ADAPTER.JDBC_ADAPTER";
        stmt.execute("GRANT EXECUTE ON " + adapterName + " TO " + userName);
        stmt.execute("GRANT ACCESS ON CONNECTION " + CONNECTION_NAME + " TO " + userName);
        stmt.execute("GRANT CONNECTION " + CONNECTION_NAME + " TO " + userName);
        stmt.execute("COMMIT");
        final Connection conn2 = connectToExa(userName, userName);
        final Statement stmt2 = conn2.createStatement();
        createVirtualSchema(conn2, VIRTUAL_SCHEMA, "EXASOL", "", "default", CONNECTION_NAME, "", "", adapterName, "",
                false, getConfig().debugAddress(), "", null, "");
        final ResultSet result = stmt2.executeQuery("SELECT * FROM \"sample_07\"");
        result.next();
        assertEquals("00-0000", result.getString(1));
        stmt.execute("DROP USER IF EXISTS " + userName + " CASCADE");

    }

    private static void createKerberosJDBCAdapter() throws SQLException, FileNotFoundException {
        final List<String> kerberosIncludes = new ArrayList<>();
        kerberosIncludes.add(getConfig().getJdbcAdapterPath());
        final String jdbcPrefixPath = getConfig().getKerberosJdbcPrefixPath();
        for (final String jar : getConfig().getKerberosJdbcJars()) {
            kerberosIncludes.add(jdbcPrefixPath + jar);
        }
        createJDBCAdapter(kerberosIncludes);
    }
}
