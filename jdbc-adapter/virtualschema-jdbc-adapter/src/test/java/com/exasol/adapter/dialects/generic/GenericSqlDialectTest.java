package com.exasol.adapter.dialects.generic;

import static com.exasol.adapter.AdapterProperties.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.PropertyValidationException;

class GenericSqlDialectTest {

    @Test
    void testValidateCatalogProperty() throws PropertyValidationException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "GENERIC");
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        properties.put(CATALOG_NAME_PROPERTY, "MY_CATALOG");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new GenericSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testValidateDialectNameProperty() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "ORACLE");
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new GenericSqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        MatcherAssert.assertThat(exception.getMessage(), containsString(
                "The dialect GENERIC cannot have the name ORACLE. You specified the wrong dialect name or created the wrong dialect class."));
    }

    @Test
    void testValidateSchemaProperty() throws PropertyValidationException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "GENERIC");
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        properties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new GenericSqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }
}