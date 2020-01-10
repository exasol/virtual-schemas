package com.exasol.adapter.dialects.generic;

import static com.exasol.adapter.AdapterProperties.CATALOG_NAME_PROPERTY;
import static com.exasol.adapter.AdapterProperties.SCHEMA_NAME_PROPERTY;
import static org.mockito.Mockito.when;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.PropertyValidationException;
import com.exasol.adapter.dialects.SqlDialect;

@ExtendWith(MockitoExtension.class)
class GenericSqlDialectTest {
    private Map<String, String> rawProperties;
    @Mock
    private Connection connectionMock;
    @Mock
    private DatabaseMetaData metadataMock;

    @BeforeEach
    void beforeEach() throws SQLException {
        this.rawProperties = new HashMap<>();
        when(this.connectionMock.getMetaData()).thenReturn(this.metadataMock);
        when(this.metadataMock.supportsMixedCaseIdentifiers()).thenReturn(true);
        when(this.metadataMock.supportsMixedCaseQuotedIdentifiers()).thenReturn(true);
    }

    @Test
    void testValidateCatalogProperty() throws PropertyValidationException {
        setMandatoryProperties("GENERIC");
        this.rawProperties.put(CATALOG_NAME_PROPERTY, "MY_CATALOG");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new GenericSqlDialect(this.connectionMock, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        setMandatoryProperties("GENERIC");
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new GenericSqlDialect(this.connectionMock, adapterProperties);
        sqlDialect.validateProperties();
    }

    private void setMandatoryProperties(final String sqlDialectProperty) {
        this.rawProperties.put(AdapterProperties.SQL_DIALECT_PROPERTY, sqlDialectProperty);
        this.rawProperties.put(AdapterProperties.CONNECTION_NAME_PROPERTY, "MY_CONN");
    }
}