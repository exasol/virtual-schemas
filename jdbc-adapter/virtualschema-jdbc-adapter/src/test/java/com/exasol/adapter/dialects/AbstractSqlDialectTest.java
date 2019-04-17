package com.exasol.adapter.dialects;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.*;

import com.exasol.adapter.jdbc.JdbcAdapterProperties;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.jdbc.PropertyValidationException;

class AbstractSqlDialectTest {
    private final SqlDialect sqlDialect = new DummySqlDialect(null, AdapterProperties.emptyProperties());

    @Test
    void testNoCredentials() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(AbstractSqlDialect.SQL_DIALECT_PROPERTY, "GENERIC");
        properties.put(AbstractSqlDialect.SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                () -> this.sqlDialect.validateProperties(properties));
        assertThat(exception.getMessage(),
                containsString(
                        "You did not specify a connection (CONNECTION_NAME) and therefore have to specify the property "
                                + "CONNECTION_STRING"));
    }

    @Test
    void testUserNamePasswordOptional() throws PropertyValidationException {
        final Map<String, String> properties = getMinimumMandatory();
        this.sqlDialect.validateProperties(properties);
    }

    private static Map<String, String> getMinimumMandatory() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(AbstractSqlDialect.SQL_DIALECT_PROPERTY, "GENERIC");
        properties.put(AbstractSqlDialect.CONNECTION_NAME_PROPERTY, "MY_CONN");
        return properties;
    }

    @Test
    void testRedundantCredentialsUserName() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(AbstractSqlDialect.USERNAME_PROPERTY, "MY_USER");
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                () -> this.sqlDialect.validateProperties(properties));
        assertThat(exception.getMessage(),
                containsString("You specified a connection (CONNECTION_NAME) and therefore may not specify "));
    }

    @Test
    void testRedundantCredentialsPassword() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(AbstractSqlDialect.PASSWORD_PROPERTY, "MY_PASSWORD");
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                () -> this.sqlDialect.validateProperties(properties));
        assertThat(exception.getMessage(),
                containsString("You specified a connection (CONNECTION_NAME) and therefore may not specify "));
    }

    @Test
    void testRedundantCredentialsConnectionString() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(AbstractSqlDialect.CONNECTION_STRING_PROPERTY, "MY_CONN");
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                () -> this.sqlDialect.validateProperties(properties));
        assertThat(exception.getMessage(),
                containsString("You specified a connection (CONNECTION_NAME) and therefore may not specify "));
    }

    @Test
    void testNoDialect() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(AbstractSqlDialect.CONNECTION_NAME_PROPERTY, "MY_CONN");
        properties.put(AbstractSqlDialect.SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                () -> this.sqlDialect.validateProperties(properties));
        assertThat(exception.getMessage(), containsString("You have to specify the SQL dialect "));
    }

    @Test
    void testInvalidDialect() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(AbstractSqlDialect.CONNECTION_NAME_PROPERTY, "MY_CONN");
        properties.put(AbstractSqlDialect.SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        properties.put(AbstractSqlDialect.SQL_DIALECT_PROPERTY, "INVALID_DIALECT");
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                () -> this.sqlDialect.validateProperties(properties));
        assertThat(exception.getMessage(), containsString("SQL Dialect \"INVALID_DIALECT\" is not supported."));
    }

    @Test
    void testInvalidDebugAddress1() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(AbstractSqlDialect.DEBUG_ADDRESS_PROPERTY, "bla");
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                () -> this.sqlDialect.validateProperties(properties));
        assertThat(exception.getMessage(), containsString("You specified an invalid hostname and port"));
    }

    @Test
    void testInvalidDebugAddress2() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(AbstractSqlDialect.DEBUG_ADDRESS_PROPERTY, "bla:no-number");
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                () -> this.sqlDialect.validateProperties(properties));
        assertThat(exception.getMessage(), containsString("You specified an invalid hostname and port"));
    }

    @Test
    void testInvalidDebugAddress3() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(AbstractSqlDialect.DEBUG_ADDRESS_PROPERTY, "bla:123:456");
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                () -> this.sqlDialect.validateProperties(properties));
        assertThat(exception.getMessage(), containsString("You specified an invalid hostname and port"));
    }

    @Test
    void testValidDebugAddress() throws PropertyValidationException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(AbstractSqlDialect.DEBUG_ADDRESS_PROPERTY, "bla:123");
        this.sqlDialect.validateProperties(properties);
    }

    @Test
    void testSchemaAndCatalogOptional() throws PropertyValidationException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(AbstractSqlDialect.SQL_DIALECT_PROPERTY, "GENERIC");
        properties.put(AbstractSqlDialect.CONNECTION_NAME_PROPERTY, "MY_CONN");
        this.sqlDialect.validateProperties(properties);
    }

    @Test
    void checkValidBoolOptions() throws PropertyValidationException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(AbstractSqlDialect.IS_LOCAL_PROPERTY, "TrUe");
        this.sqlDialect.validateProperties(properties);

        properties = getMinimumMandatory();
        properties.put(AbstractSqlDialect.IS_LOCAL_PROPERTY, "FalSe");
        this.sqlDialect.validateProperties(properties);
    }

    @Test
    void checkInvalidBoolOption1() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(AbstractSqlDialect.IS_LOCAL_PROPERTY, "asdasd");
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                () -> this.sqlDialect.validateProperties(properties));
        assertThat(exception.getMessage(), containsString(
                "The value 'asdasd' for the property IS_LOCAL is invalid. It has to be either 'true' or 'false' (case "
                        + "insensitive)"));
    }

    @Test
    void testInvalidExceptionHandling() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(AbstractSqlDialect.EXCEPTION_HANDLING_PROPERTY, "IGNORE_ALL");
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                () -> this.sqlDialect.validateProperties(properties));
        assertThat(exception.getMessage(), containsString("You specified an invalid exception mode (IGNORE_ALL)"));
    }

    @Test
    void getIgnoreErrors() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("IGNORE_ERRORS", "ERrror_foo, error_bar    ,  another_error, уккщк");
        final List<String> expectedErrorList = new ArrayList<>();
        expectedErrorList.add("ERRROR_FOO");
        expectedErrorList.add("ERROR_BAR");
        expectedErrorList.add("ANOTHER_ERROR");
        expectedErrorList.add("УККЩК");
        assertEquals(expectedErrorList, AbstractSqlDialect.getIgnoreErrorList(properties));
    }
}