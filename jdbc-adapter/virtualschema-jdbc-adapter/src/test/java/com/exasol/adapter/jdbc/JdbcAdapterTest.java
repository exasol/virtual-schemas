package com.exasol.adapter.jdbc;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.exasol.*;
import com.exasol.adapter.dialects.JdbcTypeDescription;
import com.exasol.adapter.metadata.DataType;
import com.exasol.adapter.metadata.SchemaMetadataInfo;

public class JdbcAdapterTest {
    private ExaMetadata exaMetadata;

    private Map<String, String> propertiesConnectionName;
    private Map<String, String> propertiesConnectionStringUserPassword;

    @Before
    public void setUp() throws ExaConnectionAccessException {
        final ExaConnectionInformation exaConnectionInformation = mock(ExaConnectionInformation.class);
        this.exaMetadata = mock(ExaMetadata.class);

        when(exaConnectionInformation.getUser()).thenReturn("testCommUser");
        when(exaConnectionInformation.getPassword()).thenReturn("testConnPassword");
        when(exaConnectionInformation.getAddress()).thenReturn("testConnAddress");
        when(this.exaMetadata.getConnection(any())).thenReturn(exaConnectionInformation);

        this.propertiesConnectionStringUserPassword = new HashMap<>();
        this.propertiesConnectionStringUserPassword.put("CONNECTION_STRING", "testConnectionString");
        this.propertiesConnectionStringUserPassword.put("USERNAME", "testUsername");
        this.propertiesConnectionStringUserPassword.put("PASSWORD", "testPassword");

        this.propertiesConnectionName = new HashMap<>();
        this.propertiesConnectionName.put("CONNECTION_NAME", "CONN_NAME");

    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionNameGiven() {
        final SchemaMetadataInfo schemaMetadataInfoConnectionName = new SchemaMetadataInfo("", "",
                this.propertiesConnectionName);
        final String credentials = new JdbcAdapter().getCredentialsForPushdownQuery(this.exaMetadata,
                schemaMetadataInfoConnectionName);
        assertEquals("CONN_NAME", credentials);
    }

    @Test
    public void getCredentialsForORAImportWithConnectionNameGiven() {
        final Map<String, String> oraPropertiesConnectionName = new HashMap<>(this.propertiesConnectionName);
        oraPropertiesConnectionName.put("IMPORT_FROM_ORA", "true");
        final SchemaMetadataInfo oraSchemaMetadataInfoConnectionName = new SchemaMetadataInfo("", "",
                oraPropertiesConnectionName);
        final String credentials = new JdbcAdapter().getCredentialsForPushdownQuery(this.exaMetadata,
                oraSchemaMetadataInfoConnectionName);
        assertEquals("", credentials);
    }

    @Test
    public void getCredentialsForEXAImportWithConnectionNameGiven() {
        final Map<String, String> exaPropertiesConnectionName = new HashMap<>(this.propertiesConnectionName);
        exaPropertiesConnectionName.put("IMPORT_FROM_EXA", "true");
        final SchemaMetadataInfo exaSchemaMetadataInfoConnectionName = new SchemaMetadataInfo("", "",
                exaPropertiesConnectionName);

        final String credentials = new JdbcAdapter().getCredentialsForPushdownQuery(this.exaMetadata,
                exaSchemaMetadataInfoConnectionName);
        assertEquals("USER 'testCommUser' IDENTIFIED BY 'testConnPassword'", credentials);
    }

    @Test
    public void getCredentialsForJDBCImportWithConnectionStringUsernamePasswordGiven() {
        final SchemaMetadataInfo schemaMetadataInfoConnectionStringUserPassword = new SchemaMetadataInfo("", "",
                this.propertiesConnectionStringUserPassword);
        final String credentials = new JdbcAdapter().getCredentialsForPushdownQuery(this.exaMetadata,
                schemaMetadataInfoConnectionStringUserPassword);
        assertEquals("'testConnectionString' USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void getCredentialsForORAImportWithConnectionStringUsernamePasswordGiven() {
        final Map<String, String> oraPropertiesConnectionStringUserPassword = new HashMap<>(
                this.propertiesConnectionStringUserPassword);
        oraPropertiesConnectionStringUserPassword.put("IMPORT_FROM_ORA", "true");
        final SchemaMetadataInfo oraSchemaMetadataInfoConnectionStringUserPassword = new SchemaMetadataInfo("", "",
                oraPropertiesConnectionStringUserPassword);
        final String credentials = new JdbcAdapter().getCredentialsForPushdownQuery(this.exaMetadata,
                oraSchemaMetadataInfoConnectionStringUserPassword);
        assertEquals("USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void getCredentialsForEXAImportWithConnectionStringUsernamePasswordGiven() {
        final Map<String, String> exaPropertiesConnectionStringUserPassword = new HashMap<>(
                this.propertiesConnectionStringUserPassword);
        exaPropertiesConnectionStringUserPassword.put("IMPORT_FROM_EXA", "true");
        final SchemaMetadataInfo exaSchemaMetadataInfoConnectionStringUserPassword = new SchemaMetadataInfo("", "",
                exaPropertiesConnectionStringUserPassword);
        final String credentials = new JdbcAdapter().getCredentialsForPushdownQuery(this.exaMetadata,
                exaSchemaMetadataInfoConnectionStringUserPassword);
        assertEquals("USER 'testUsername' IDENTIFIED BY 'testPassword'", credentials);
    }

    @Test
    public void buildColumnDescription() {
        final DataType[] types = new DataType[3];
        types[0] = DataType.createBool();
        types[1] = DataType.createDecimal(10, 2);
        types[2] = DataType.createTimestamp(false);
        assertEquals("(c0 BOOLEAN,c1 DECIMAL(10, 2),c2 TIMESTAMP)", JdbcAdapter.buildColumnDescriptionFrom(types));
    }

    @Test
    public void buildJdbcTypeDescriptionFromResultSetMetadata() throws SQLException {
        final ResultSetMetaData metadata = Mockito.mock(ResultSetMetaData.class);
        when(metadata.getColumnType(0)).thenReturn(Types.SMALLINT);
        when(metadata.getPrecision(0)).thenReturn(9);
        when(metadata.getScale(0)).thenReturn(0);
        when(metadata.getColumnTypeName(0)).thenReturn("SMALLINT");

        final JdbcTypeDescription type = JdbcAdapter.getJdbcTypeDescription(metadata, 0);
        assertEquals(Types.SMALLINT, type.getJdbcType());
        assertEquals(9, type.getPrecisionOrSize());
        assertEquals(0, type.getDecimalScale());
        assertEquals("SMALLINT", type.getTypeName());
        assertEquals(0, type.getCharOctetLength());
    }
}