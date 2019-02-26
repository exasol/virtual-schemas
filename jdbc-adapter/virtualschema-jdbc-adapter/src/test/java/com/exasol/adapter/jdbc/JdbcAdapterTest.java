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
    private SchemaMetadataInfo schemaMetadataInfoConnectionStringUserPassword;
    private SchemaMetadataInfo schemaMetadataInfoConnectionName;


    @Before
    public void setUp() throws ExaConnectionAccessException {
        exaConnectionInformation = mock(ExaConnectionInformation.class);
        exaMetadata = mock(ExaMetadata.class);

        when(exaConnectionInformation.getUser()).thenReturn("testCommUser");
        when(exaConnectionInformation.getPassword()).thenReturn("testConnPassword");
        when(exaConnectionInformation.getAddress()).thenReturn("testConnAddress");
        when(exaMetadata.getConnection(any())).thenReturn(exaConnectionInformation);

        Map<String, String> propertiesConnectionStringUserPassword = new HashMap<>();
        propertiesConnectionStringUserPassword.put("CONNECTION_STRING", "testConnectionString");
        propertiesConnectionStringUserPassword.put("USERNAME", "testUsername");
        propertiesConnectionStringUserPassword.put("PASSWORD", "testPassword");
        schemaMetadataInfoConnectionStringUserPassword = new SchemaMetadataInfo("", "", propertiesConnectionStringUserPassword);

        Map<String, String> propertiesConnectionName = new HashMap<>();
        propertiesConnectionName.put("CONNECTION_NAME", "CONN_NAME");
        schemaMetadataInfoConnectionName = new SchemaMetadataInfo("", "", propertiesConnectionName);
    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionNameGiven() {
        final String credentials = JdbcAdapter.getCredentialsForJDBCImport(exaMetadata, schemaMetadataInfoConnectionName);
        assertEquals("CONN_NAME", credentials);
    }

    @Test
    public void getCredentialsForORAImportWithConnectionNameGiven() {
        final String credentials = JdbcAdapter.getCredentialsForORAImport(exaMetadata, schemaMetadataInfoConnectionName);
        assertEquals("", credentials);
    }

    @Test
    public void getCredentialsForEXAImportWithConnectionNameGiven() {
        final String credentials = JdbcAdapter.getCredentialsForEXAImport(exaMetadata, schemaMetadataInfoConnectionName);
        assertEquals("USER 'testCommUser' IDENTIFIED BY 'testConnPassword'", credentials);
    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionStringUsernamePasswordGiven() {
        final String credentials = JdbcAdapter.getCredentialsForJDBCImport(exaMetadata, schemaMetadataInfoConnectionStringUserPassword);
        assertEquals("'testConnectionString' USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void getCredentialsForORAImportWithConnectionStringUsernamePasswordGiven() {
        final String credentials = JdbcAdapter.getCredentialsForORAImport(exaMetadata, schemaMetadataInfoConnectionStringUserPassword);
        assertEquals("USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void getCredentialsForEXAImportWithConnectionStringUsernamePasswordGiven() {
        final String credentials = JdbcAdapter.getCredentialsForEXAImport(exaMetadata, schemaMetadataInfoConnectionStringUserPassword);
        assertEquals("USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }
}