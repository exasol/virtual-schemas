package com.exasol.adapter.dialects;

import static com.exasol.adapter.AdapterProperties.*;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.*;

import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.PropertyValidationException;

class AbstractSqlDialectTest {
    @Test
    void testNoCredentials() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "GENERIC");
        properties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString(
                        "You did not specify a connection (CONNECTION_NAME) and therefore have to specify the property "
                                + "CONNECTION_STRING"));
    }

    @Test
    void testUserNamePasswordOptional() throws PropertyValidationException {
        final Map<String, String> properties = getMinimumMandatory();
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    private static Map<String, String> getMinimumMandatory() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "GENERIC");
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        return properties;
    }

    @Test
    void testRedundantCredentialsUserName() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(USERNAME_PROPERTY, "MY_USER");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString("You specified a connection (CONNECTION_NAME) and therefore may not specify "));
    }

    @Test
    void testRedundantCredentialsPassword() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(PASSWORD_PROPERTY, "MY_PASSWORD");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString("You specified a connection (CONNECTION_NAME) and therefore may not specify "));
    }

    @Test
    void testRedundantCredentialsConnectionString() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(CONNECTION_STRING_PROPERTY, "MY_CONN");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString("You specified a connection (CONNECTION_NAME) and therefore may not specify "));
    }

    @Test
    void testNoDialect() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        properties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("You have to specify the SQL dialect "));
    }

    @Test
    void testInvalidDialect() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        properties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        properties.put(SQL_DIALECT_PROPERTY, "INVALID_DIALECT");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("SQL Dialect \"INVALID_DIALECT\" is not supported."));
    }

    @Test
    void testInvalidDebugAddress1() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(DEBUG_ADDRESS_PROPERTY, "bla");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("You specified an invalid hostname and port"));
    }

    @Test
    void testInvalidDebugAddress2() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(DEBUG_ADDRESS_PROPERTY, "bla:no-number");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("You specified an invalid hostname and port"));
    }

    @Test
    void testInvalidDebugAddress3() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(DEBUG_ADDRESS_PROPERTY, "bla:123:456");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("You specified an invalid hostname and port"));
    }

    @Test
    void testValidDebugAddress() throws PropertyValidationException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(DEBUG_ADDRESS_PROPERTY, "bla:123");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testSchemaAndCatalogOptional() throws PropertyValidationException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(SQL_DIALECT_PROPERTY, "GENERIC");
        properties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void checkValidBoolOptions() throws PropertyValidationException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(IS_LOCAL_PROPERTY, "TrUe");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();

        properties = getMinimumMandatory();
        properties.put(IS_LOCAL_PROPERTY, "FalSe");
        final AdapterProperties adapterProperties2 = new AdapterProperties(properties);
        final SqlDialect sqlDialect2 = new DummySqlDialect(null, adapterProperties);
        sqlDialect2.validateProperties();
    }

    @Test
    void checkInvalidBoolOption1() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(IS_LOCAL_PROPERTY, "asdasd");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString(
                "The value 'asdasd' for the property IS_LOCAL is invalid. It has to be either 'true' or 'false' (case "
                        + "insensitive)"));
    }

    @Test
    void testInvalidExceptionHandling() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(EXCEPTION_HANDLING_PROPERTY, "IGNORE_ALL");
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("You specified an invalid exception mode (IGNORE_ALL)"));
    }

    @Test
    void getIgnoreErrors() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("IGNORE_ERRORS", "ERrror_foo, error_bar    ,  another_error, уккщк");
        final List<String> expectedErrorList = new ArrayList<>();
        final AdapterProperties adapterProperties = new AdapterProperties(properties);
        final DummySqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        expectedErrorList.add("ERRROR_FOO");
        expectedErrorList.add("ERROR_BAR");
        expectedErrorList.add("ANOTHER_ERROR");
        expectedErrorList.add("УККЩК");
        assertEquals(expectedErrorList, sqlDialect.getIgnoredErrors());
    }
}