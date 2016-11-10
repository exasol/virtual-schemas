package com.exasol.adapter.dialects.impl;

import com.exasol.adapter.dialects.AbstractIntegrationTest;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Integration test for JDBC drivers requiring Kerberos authentication. This is currently only tested for the Cloudera Hive JDBC driver developed by Simba (probably also works for the Cloudera Impala Driver developed by Simba)
 */
public class KerberosIT extends AbstractIntegrationTest {

    private static final String VIRTUAL_SCHEMA = "VS_KERBEROS_IT";
    private static final String CONNECTION_NAME = "krb_conn";
    private static final boolean IS_LOCAL = false;

    @BeforeClass
    public static void setUpClass() throws FileNotFoundException, SQLException, ClassNotFoundException {
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
    public void testKerberosVirtualSchema() throws SQLException, ClassNotFoundException, FileNotFoundException {
        createVirtualSchema(
                VIRTUAL_SCHEMA,
                ExasolSqlDialect.NAME,
                "",
                "default",
                CONNECTION_NAME,
                "",
                "",
                "ADAPTER.JDBC_ADAPTER",
                "", IS_LOCAL,
                getConfig().debugAddress(),
                "");
        Statement stmt = getConnection().createStatement();
        ResultSet result = stmt.executeQuery("SELECT * FROM \"sample_07\"");
        result.next();
        assertEquals("00-0000", result.getString(1));
    }

    @Test
    public void testKerberosVirtualSchemaGrantConnection() throws SQLException, ClassNotFoundException, FileNotFoundException {
        // Create Kerberos Virtual Schema using a different user, which has the appropriate privileges for the connection
        final String userName = "user1";
        Statement stmt = getConnection().createStatement();
        stmt.execute("DROP USER IF EXISTS " + userName +" CASCADE");
        stmt.execute("CREATE USER " + userName +" identified by \"" + userName + "\"");
        stmt.execute("GRANT CREATE SESSION TO " + userName);
        stmt.execute("GRANT CREATE VIRTUAL SCHEMA TO " + userName);
        stmt.execute("GRANT DROP ANY VIRTUAL SCHEMA TO " + userName);
        final String adapterName = "ADAPTER.JDBC_ADAPTER";
        stmt.execute("GRANT EXECUTE ON " + adapterName + " TO " + userName);
        stmt.execute("GRANT ACCESS ON CONNECTION " + CONNECTION_NAME + " TO " + userName);
        stmt.execute("GRANT CONNECTION " + CONNECTION_NAME + " TO " + userName);
        stmt.execute("COMMIT");
        Connection conn2 = connectToExa(userName, userName);
        Statement stmt2 = conn2.createStatement();
        createVirtualSchema(
                conn2,
                VIRTUAL_SCHEMA,
                ExasolSqlDialect.NAME,
                "",
                "default",
                CONNECTION_NAME,
                "",
                "",
                adapterName,
                "", false,
                getConfig().debugAddress(),
                "");
        ResultSet result = stmt2.executeQuery("SELECT * FROM \"sample_07\"");
        result.next();
        assertEquals("00-0000", result.getString(1));
        stmt.execute("DROP USER IF EXISTS " + userName +" CASCADE");

    }

    private static void createKerberosJDBCAdapter() throws SQLException, FileNotFoundException {
        List<String> kerberosIncludes = new ArrayList<>();
        kerberosIncludes.add(getConfig().getJdbcAdapterPath());
        String jdbcPrefixPath = getConfig().getKerberosJdbcPrefixPath();
        for (String jar : getConfig().getKerberosJdbcJars()) {
            kerberosIncludes.add(jdbcPrefixPath + jar);
        }
        createJDBCAdapter(kerberosIncludes);
    }

}
