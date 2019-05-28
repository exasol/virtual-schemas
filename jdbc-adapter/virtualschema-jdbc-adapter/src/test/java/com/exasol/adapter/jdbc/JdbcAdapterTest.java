package com.exasol.adapter.jdbc;

import static com.exasol.adapter.AdapterProperties.*;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.capabilities.MainCapability;
import com.exasol.adapter.dialects.generic.GenericSqlDialect;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.request.GetCapabilitiesRequest;
import com.exasol.adapter.request.PushDownRequest;
import com.exasol.adapter.response.GetCapabilitiesResponse;
import com.exasol.adapter.response.PushDownResponse;
import com.exasol.adapter.sql.SqlStatement;
import com.exasol.adapter.sql.TestSqlStatementFactory;

public class JdbcAdapterTest {
    private static final String SCHEMA_NAME = "THE_SCHEMA";
    private final JdbcAdapter adapter = new JdbcAdapter();
    private Map<String, String> rawProperties;
    private final String GENERIC_ADAPTER_NAME = GenericSqlDialect.getPublicName();

    @BeforeEach
    void beforeEach() {
        this.rawProperties = new HashMap<>();
    }

    @Test
    public void testPushdown() throws AdapterException {
        final PushDownResponse response = pushStatementDown(TestSqlStatementFactory.createSelectOneFromSysDummy());
        assertThat(response.getPushDownSql(), equalTo("IMPORT INTO (c1 DECIMAL(10, 0))" //
                + " FROM JDBC" //
                + " AT 'jdbc:derby:memory:test;create=true;' USER '' IDENTIFIED BY ''"//
                + " STATEMENT 'SELECT 1 FROM \"SYSIBM\".\"SYSDUMMY1\"'"));
    }

    private PushDownResponse pushStatementDown(final SqlStatement statement) throws AdapterException {
        setGenericSqlDialectProperty();
        setDerbyConnectionProperties();
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "SYSIBM");
        final List<TableMetadata> involvedTablesMetadata = null;
        final PushDownRequest request = new PushDownRequest(this.GENERIC_ADAPTER_NAME, createSchemaMetadataInfo(),
                statement, involvedTablesMetadata);
        final ExaMetadata exaMetadataMock = Mockito.mock(ExaMetadata.class);
        final PushDownResponse response = this.adapter.pushdown(exaMetadataMock, request);
        return response;
    }

    private void setGenericSqlDialectProperty() {
        this.rawProperties.put(SQL_DIALECT_PROPERTY, this.GENERIC_ADAPTER_NAME);
    }

    private void setDerbyConnectionProperties() {
        this.rawProperties.put(CONNECTION_STRING_PROPERTY, "jdbc:derby:memory:test;create=true;");
        this.rawProperties.put(USERNAME_PROPERTY, "");
        this.rawProperties.put(PASSWORD_PROPERTY, "");
    }

    private SchemaMetadataInfo createSchemaMetadataInfo() {
        return new SchemaMetadataInfo(SCHEMA_NAME, "", this.rawProperties);
    }

    @Test
    public void testPushdownWithIllegalStatementThrowsException() throws AdapterException {
        assertThrows(AdapterException.class,
                () -> pushStatementDown(TestSqlStatementFactory.createSelectOneFromDual()));
    }

    @Test
    public void testGetCapabilities() throws AdapterException {
        setGenericSqlDialectProperty();
        setDerbyConnectionProperties();
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "SYSIBM");
        final GetCapabilitiesRequest request = new GetCapabilitiesRequest(this.GENERIC_ADAPTER_NAME,
                createSchemaMetadataInfo());
        final ExaMetadata exaMetadataMock = Mockito.mock(ExaMetadata.class);
        final GetCapabilitiesResponse response = this.adapter.getCapabilities(exaMetadataMock, request);
        assertThat(response.getCapabilities().getMainCapabilities(), emptyCollectionOf(MainCapability.class));
    }
}
