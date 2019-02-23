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
    private Map<String, String> propertiesConnectionStringUserPassword;
    private Map<String, String> propertiesConnectionName;

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
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", propertiesConnectionName);
        final String credentials = JdbcAdapter.getCredentialsForJDBCImport(exaMetadata, schemaMetadataInfo);
        assertEquals("CONN_NAME", credentials);
    }

    @Test
    public void getCredentialsForORAImportWithConnectionNameGiven() {
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", propertiesConnectionName);
        final String credentials = JdbcAdapter.getCredentialsForORAImport(exaMetadata, schemaMetadataInfo);
        assertEquals("", credentials);
    }

    @Test
    public void getCredentialsForEXAImportWithConnectionNameGiven() {
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", propertiesConnectionName);
        final String credentials = JdbcAdapter.getCredentialsForEXAImport(exaMetadata, schemaMetadataInfo);
        assertEquals("USER 'testCommUser' IDENTIFIED BY 'testConnPassword'", credentials);
    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionStringUsernamePasswordGiven() {
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", propertiesConnectionStringUserPassword);
        final String credentials = JdbcAdapter.getCredentialsForJDBCImport(exaMetadata, schemaMetadataInfo);
        assertEquals("'testConnectionString' USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void getCredentialsForORAImportWithConnectionStringUsernamePasswordGiven() {
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", propertiesConnectionStringUserPassword);
        final String credentials = JdbcAdapter.getCredentialsForORAImport(exaMetadata, schemaMetadataInfo);
        assertEquals("USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void getCredentialsForEXAImportWithConnectionStringUsernamePasswordGiven() {
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", propertiesConnectionStringUserPassword);
        final String credentials = JdbcAdapter.getCredentialsForEXAImport(exaMetadata, schemaMetadataInfo);
        assertEquals("USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }
}