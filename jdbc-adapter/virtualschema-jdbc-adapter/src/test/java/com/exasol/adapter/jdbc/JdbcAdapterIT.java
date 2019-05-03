package com.exasol.adapter.jdbc;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.exasol.adapter.*;
import com.exasol.adapter.response.GetCapabilitiesResponse;

class JdbcAdapterIT {
    private static final String DEFAULT_REQUEST_PARTS = "    \"schemaMetadataInfo\" :\n" //
            + "    {\n" //
            + "        \"name\" : \"foo\",\n" //
            + "        \"properties\" :\n" //
            + "        {\n" //
            + "            \"SQL_DIALECT\" : \"EXASOL\"\n" //
            + "        }\n" //
            + "    }\n";

    @Test
    public void testRegisteredDialects() throws AdapterException {
        final String rawRequest = "{\n" //
                + "    \"type\" : \"getCapabilities\",\n" //
                + DEFAULT_REQUEST_PARTS //
                + "}";
        final VirtualSchemaAdapter adapterMock = Mockito.mock(VirtualSchemaAdapter.class);
        when(adapterMock.getCapabilities(any(), any())).thenReturn(GetCapabilitiesResponse.builder().build());
        RequestDispatcher.adapterCall(null, rawRequest);
        assertThat(AdapterRegistry.getInstance().getRegisteredAdapterFactories(), contains(JdbcAdapterFactory.class));
    }
}