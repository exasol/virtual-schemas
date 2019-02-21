package com.exasol.adapter.jdbc;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.metadata.SchemaMetadata;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

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

        when(exaConnectionInformation.getUser()).thenReturn("testUser");
        when(exaConnectionInformation.getPassword()).thenReturn("testPassword");
        when(exaConnectionInformation.getAddress()).thenReturn("testAddress");
        when(exaMetadata.getConnection(any())).thenReturn(exaConnectionInformation);
    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionNameGiven() {
        Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_NAME", "JDBC_ORACLE");
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", properties);
        final String credentials = JdbcAdapter.getCredentialsForImport(exaMetadata, schemaMetadataInfo, true);
        assertEquals("JDBC_ORACLE", credentials);
    }

    @Test
    public void getCredentialsForORAorEXAImportWithConnectionNameGiven() {
        Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_NAME", "JDBC_ORACLE");
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", properties);
        final String credentials = JdbcAdapter.getCredentialsForImport(exaMetadata, schemaMetadataInfo, false);
        assertEquals("", credentials);
    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionStringUsernamePasswordGiven() {
        Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_STRING", "testConnectionString");
        properties.put("USERNAME", "testUsername");
        properties.put("PASSWORD", "testPassword");
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", properties);
        final String credentials = JdbcAdapter.getCredentialsForImport(exaMetadata, schemaMetadataInfo, true);
        assertEquals("'testConnectionString' USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void getCredentialsForORAorEXAImportWithConnectionStringUsernamePasswordGiven() {
        Map<String, String> properties = new HashMap<>();
        properties.put("CONNECTION_STRING", "testConnectionString");
        properties.put("USERNAME", "testUsername");
        properties.put("PASSWORD", "testPassword");
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("", "", properties);
        final String credentials = JdbcAdapter.getCredentialsForImport(exaMetadata, schemaMetadataInfo, false);
        assertEquals("USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }
}