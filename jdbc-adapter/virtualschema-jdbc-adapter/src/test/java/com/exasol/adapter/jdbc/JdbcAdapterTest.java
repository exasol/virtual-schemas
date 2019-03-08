package com.exasol.adapter.jdbc;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JdbcAdapterTest {
    private ExaConnectionInformation exaConnectionInformation;
    private ExaMetadata exaMetadata;

    private Map<String, String> propertiesConnectionName;
    private Map<String, String> propertiesConnectionStringUserPassword;


    @Before
    public void setUp() throws ExaConnectionAccessException {
        exaConnectionInformation = mock(ExaConnectionInformation.class);
        exaMetadata = mock(ExaMetadata.class);

        when(exaConnectionInformation.getUser()).thenReturn("testCommUser");
        when(exaConnectionInformation.getPassword()).thenReturn("testConnPassword");
        when(exaConnectionInformation.getAddress()).thenReturn("testConnAddress");
        when(exaMetadata.getConnection(any())).thenReturn(exaConnectionInformation);

        propertiesConnectionStringUserPassword = new HashMap<>();
        propertiesConnectionStringUserPassword.put("CONNECTION_STRING", "testConnectionString");
        propertiesConnectionStringUserPassword.put("USERNAME", "testUsername");
        propertiesConnectionStringUserPassword.put("PASSWORD", "testPassword");

        propertiesConnectionName = new HashMap<>();
        propertiesConnectionName.put("CONNECTION_NAME", "CONN_NAME");


    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionNameGiven() {
        final SchemaMetadataInfo schemaMetadataInfoConnectionName = new SchemaMetadataInfo("", "", propertiesConnectionName);
        final String credentials = JdbcAdapter.getCredentialsForPushdownQuery(exaMetadata, schemaMetadataInfoConnectionName);
        assertEquals("CONN_NAME", credentials);
    }

    @Test
    public void getCredentialsForORAImportWithConnectionNameGiven() {
        Map<String, String> oraPropertiesConnectionName = new HashMap<>(propertiesConnectionName);
        oraPropertiesConnectionName.put("IMPORT_FROM_ORA","true");
        final SchemaMetadataInfo oraSchemaMetadataInfoConnectionName = new SchemaMetadataInfo("", "", oraPropertiesConnectionName);
        final String credentials = JdbcAdapter.getCredentialsForPushdownQuery(exaMetadata, oraSchemaMetadataInfoConnectionName);
        assertEquals("", credentials);
    }

    @Test
    public void getCredentialsForEXAImportWithConnectionNameGiven() {
        Map<String, String> exaPropertiesConnectionName = new HashMap<>(propertiesConnectionName);
        exaPropertiesConnectionName.put("IMPORT_FROM_EXA","true");
        final SchemaMetadataInfo exaSchemaMetadataInfoConnectionName = new SchemaMetadataInfo("", "", exaPropertiesConnectionName);

        final String credentials = JdbcAdapter.getCredentialsForPushdownQuery(exaMetadata, exaSchemaMetadataInfoConnectionName);
        assertEquals("USER 'testCommUser' IDENTIFIED BY 'testConnPassword'", credentials);
    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionStringUsernamePasswordGiven() {
        final SchemaMetadataInfo  schemaMetadataInfoConnectionStringUserPassword = new SchemaMetadataInfo("", "", propertiesConnectionStringUserPassword);
        final String credentials = JdbcAdapter.getCredentialsForPushdownQuery(exaMetadata, schemaMetadataInfoConnectionStringUserPassword);
        assertEquals("'testConnectionString' USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void getCredentialsForORAImportWithConnectionStringUsernamePasswordGiven() {
        Map<String, String> oraPropertiesConnectionStringUserPassword = new HashMap<>(propertiesConnectionStringUserPassword);
        oraPropertiesConnectionStringUserPassword.put("IMPORT_FROM_ORA","true");
        final SchemaMetadataInfo  oraSchemaMetadataInfoConnectionStringUserPassword = new SchemaMetadataInfo("", "", oraPropertiesConnectionStringUserPassword);
        final String credentials = JdbcAdapter.getCredentialsForPushdownQuery(exaMetadata, oraSchemaMetadataInfoConnectionStringUserPassword);
        assertEquals("USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void getCredentialsForEXAImportWithConnectionStringUsernamePasswordGiven() {
        Map<String, String> exaPropertiesConnectionStringUserPassword = new HashMap<>(propertiesConnectionStringUserPassword);
        exaPropertiesConnectionStringUserPassword.put("IMPORT_FROM_EXA","true");
        final SchemaMetadataInfo  exaSchemaMetadataInfoConnectionStringUserPassword = new SchemaMetadataInfo("", "", exaPropertiesConnectionStringUserPassword);
        final String credentials = JdbcAdapter.getCredentialsForPushdownQuery(exaMetadata, exaSchemaMetadataInfoConnectionStringUserPassword);
        assertEquals("USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }
}