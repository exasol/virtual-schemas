package com.exasol.adapter.jdbc;

import static com.exasol.adapter.AdapterProperties.*;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.*;

import org.junit.Test;
import org.mockito.Mockito;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.generic.GenericSqlDialect;
import com.exasol.adapter.metadata.SchemaMetadataInfo;
import com.exasol.adapter.metadata.TableMetadata;
import com.exasol.adapter.request.PushDownRequest;
import com.exasol.adapter.response.PushDownResponse;
import com.exasol.adapter.sql.SqlStatement;
import com.exasol.adapter.sql.TestSqlStatementFactory;

public class JdbcAdapterTest {
    @Test
    public void testPushdown() throws Exception {
        final PushDownResponse response = pushStatementDown(TestSqlStatementFactory.createSelectOneFromSysDummy());
        assertThat(response.getPushDownSql(), equalTo("IMPORT INTO (c1 DECIMAL(10, 0))" //
                + " FROM JDBC" //
                + " AT 'jdbc:derby:memory:test;create=true;' USER '' IDENTIFIED BY ''"//
                + " STATEMENT 'SELECT 1 FROM \"SYSIBM\".\"SYSDUMMY1\"'"));
    }

    private PushDownResponse pushStatementDown(final SqlStatement statement) throws AdapterException {
        final JdbcAdapter adapter = new JdbcAdapter();
        final String adapterName = GenericSqlDialect.getPublicName();
        final Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTION_STRING_PROPERTY, "jdbc:derby:memory:test;create=true;");
        properties.put(USERNAME_PROPERTY, "");
        properties.put(PASSWORD_PROPERTY, "");
        properties.put(SQL_DIALECT_PROPERTY, adapterName);
        properties.put(SCHEMA_NAME_PROPERTY, "SYSIBM");
        final SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo("THE_SCHEMA", "", properties);
        final List<TableMetadata> involvedTablesMetadata = null;
        final PushDownRequest request = new PushDownRequest(adapterName, schemaMetadataInfo, statement,
                involvedTablesMetadata);
        final ExaMetadata exaMetadataMock = Mockito.mock(ExaMetadata.class);
        final PushDownResponse response = adapter.pushdown(exaMetadataMock, request);
        return response;
    }

    @Test
    public void testPushdownWithIllegalStatementThrowsException() throws Exception {
        assertThrows(AdapterException.class,
                () -> pushStatementDown(TestSqlStatementFactory.createSelectOneFromDual()));
    }
}
