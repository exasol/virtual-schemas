package com.exasol.adapter.dialects.generic;

import static com.exasol.adapter.AdapterProperties.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.*;
import com.exasol.adapter.jdbc.ConnectionFactory;

@ExtendWith(MockitoExtension.class)
class GenericSqlDialectTest {
    @Mock
    private ConnectionFactory connectionFactoryMock;

    @BeforeEach
    void beforeEach(@Mock final DatabaseMetaData metadataMock, @Mock final Connection connectionMock)
            throws SQLException {
        when(metadataMock.supportsMixedCaseIdentifiers()).thenReturn(true);
        when(metadataMock.supportsMixedCaseQuotedIdentifiers()).thenReturn(true);
        when(connectionMock.getMetaData()).thenReturn(metadataMock);
        when(this.connectionFactoryMock.getConnection()).thenReturn(connectionMock);
    }

    @Test
    void testValidateCatalogProperty() throws PropertyValidationException {
        final Map<String, String> rawProperties = Map.of( //
                CATALOG_NAME_PROPERTY, "MY_CATALOG", //
                SQL_DIALECT_PROPERTY, "GENERIC", //
                CONNECTION_NAME_PROPERTY, "MY_CONN" //
        );
        assertDialectCreatedWithValidProperties(this.connectionFactoryMock, rawProperties);
    }

    private void assertDialectCreatedWithValidProperties(final ConnectionFactory connectionFactoryMock,
            final Map<String, String> rawProperties) throws PropertyValidationException {
        final AdapterProperties adapterProperties = new AdapterProperties(rawProperties);
        final SqlDialectFactory factory = new GenericSqlDialectFactory();
        final SqlDialect sqlDialect = factory.createSqlDialect(connectionFactoryMock, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        final Map<String, String> rawProperties = Map.of( //
                SCHEMA_NAME_PROPERTY, "MY_SCHEMA", //
                SQL_DIALECT_PROPERTY, "GENERIC", //
                CONNECTION_NAME_PROPERTY, "MY_CONN" //
        );
        assertDialectCreatedWithValidProperties(this.connectionFactoryMock, rawProperties);
    }
}