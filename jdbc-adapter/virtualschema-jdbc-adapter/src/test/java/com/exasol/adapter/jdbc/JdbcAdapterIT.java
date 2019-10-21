package com.exasol.adapter.jdbc;

import static com.exasol.adapter.AdapterProperties.CONNECTION_STRING_PROPERTY;
import static com.exasol.adapter.AdapterProperties.PASSWORD_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SQL_DIALECT_PROPERTY;
import static com.exasol.adapter.AdapterProperties.USERNAME_PROPERTY;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.AdapterFactory;
import com.exasol.adapter.AdapterRegistry;
import com.exasol.adapter.RequestDispatcher;
import com.exasol.adapter.VirtualSchemaAdapter;
import com.exasol.adapter.response.GetCapabilitiesResponse;

class JdbcAdapterIT {
    @Test
    public void testRegisteredDialects() throws AdapterException {
        final String rawRequest = "{\n" //
                + "    \"type\" : \"getCapabilities\",\n" //
                + "    \"schemaMetadataInfo\" :\n" //
                + "    {\n" //
                + "        \"name\" : \"foo\",\n" //
                + "        \"properties\" :\n" //
                + "        {\n" //
                + "            \"" + SQL_DIALECT_PROPERTY + "\" : \"DERBY\"\n," //
                + "            \"" + CONNECTION_STRING_PROPERTY + "\" : \"jdbc:derby:memory:test;create=true;\"\n," //
                + "            \"" + USERNAME_PROPERTY + "\" : \"\"\n," //
                + "            \"" + PASSWORD_PROPERTY + "\" : \"\"\n" //
                + "        }\n" //
                + "    }\n" //
                + "}";
        final VirtualSchemaAdapter adapterMock = Mockito.mock(VirtualSchemaAdapter.class);
        when(adapterMock.getCapabilities(any(), any())).thenReturn(GetCapabilitiesResponse.builder().build());
        final ExaMetadata exaMetadata = Mockito.mock(ExaMetadata.class);
        RequestDispatcher.adapterCall(exaMetadata, rawRequest);
        final List<AdapterFactory> registeredFactories = AdapterRegistry.getInstance().getRegisteredAdapterFactories();
        assertThat(registeredFactories, hasItem(instanceOf(JdbcAdapterFactory.class)));
    }
}