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

    @Before
    public void setUp() throws ExaConnectionAccessException {
        exaConnectionInformation = mock(ExaConnectionInformation.class);
        exaMetadata = mock(ExaMetadata.class);

        when(exaConnectionInformation.getUser()).thenReturn("testCommUser");
        when(exaConnectionInformation.getPassword()).thenReturn("testConnPassword");
        when(exaConnectionInformation.getAddress()).thenReturn("testConnAddress");
        when(exaMetadata.getConnection(any())).thenReturn(exaConnectionInformation);
    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionNameGiven() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_NAME", "JDBC_ORACLE");
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", properties);
        final String credentials = JdbcAdapter.getCredentialsForJDBCImport(exaMetadata, schemaMetadataInfo);
        assertEquals("JDBC_ORACLE", credentials);
    }

    @Test
    public void getCredentialsForORAImportWithConnectionNameGiven() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_NAME", "JDBC_ORACLE");
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", properties);
        final String credentials = JdbcAdapter.getCredentialsForORAImport(exaMetadata, schemaMetadataInfo);
        assertEquals("", credentials);
    }

    @Test
    public void getCredentialsForEXAImportWithConnectionNameGiven() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_NAME", "JDBC_EXA");
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", properties);
        final String credentials = JdbcAdapter.getCredentialsForEXAImport(exaMetadata, schemaMetadataInfo);
        assertEquals("USER 'testCommUser' IDENTIFIED BY 'testConnPassword'", credentials);
    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionStringUsernamePasswordGiven() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_STRING", "testConnectionString");
        properties.put("USERNAME", "testUsername");
        properties.put("PASSWORD", "testPassword");
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", properties);
        final String credentials = JdbcAdapter.getCredentialsForJDBCImport(exaMetadata, schemaMetadataInfo);
        assertEquals("'testConnectionString' USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void getCredentialsForORAImportWithConnectionStringUsernamePasswordGiven() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_STRING", "testConnectionString");
        properties.put("USERNAME", "testUsername");
        properties.put("PASSWORD", "testPassword");
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", properties);
        final String credentials = JdbcAdapter.getCredentialsForORAImport(exaMetadata, schemaMetadataInfo);
        assertEquals("USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void getCredentialsForEXAImportWithConnectionStringUsernamePasswordGiven() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_STRING", "testConnectionString");
        properties.put("USERNAME", "testUsername");
        properties.put("PASSWORD", "testPassword");
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", properties);
        final String credentials = JdbcAdapter.getCredentialsForEXAImport(exaMetadata, schemaMetadataInfo);
        assertEquals("USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }
}