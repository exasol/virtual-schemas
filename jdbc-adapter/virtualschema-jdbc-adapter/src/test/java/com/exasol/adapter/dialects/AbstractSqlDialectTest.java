package com.exasol.adapter.dialects;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.dialects.exasol.ExasolSqlDialect.EXASOL_CONNECTION_STRING_PROPERTY;
import static com.exasol.adapter.dialects.exasol.ExasolSqlDialect.EXASOL_IMPORT_PROPERTY;
import static com.exasol.adapter.dialects.oracle.OracleSqlDialect.*;
import static com.exasol.adapter.dialects.postgresql.PostgreSQLSqlDialect.POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;

class AbstractSqlDialectTest {
    private Map<String, String> rawProperties;

    @BeforeEach
    void beforeEach() {
        this.rawProperties = new HashMap<>();
    }

    @Test
    void testNoCredentials() {
        this.rawProperties.put(SQL_DIALECT_PROPERTY, "GENERIC");
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
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
        getMinimumMandatory();
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    private void getMinimumMandatory() {
        this.rawProperties.put(SQL_DIALECT_PROPERTY, "GENERIC");
        this.rawProperties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
    }

    @Test
    void testRedundantCredentialsUserName() {
        getMinimumMandatory();
        this.rawProperties.put(USERNAME_PROPERTY, "MY_USER");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString("You specified a connection (CONNECTION_NAME) and therefore should not specify"));
    }

    @Test
    void testRedundantCredentialsPassword() {
        getMinimumMandatory();
        this.rawProperties.put(PASSWORD_PROPERTY, "MY_PASSWORD");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString("You specified a connection (CONNECTION_NAME) and therefore should not specify"));
    }

    @Test
    void testRedundantCredentialsConnectionString() {
        getMinimumMandatory();
        this.rawProperties.put(CONNECTION_STRING_PROPERTY, "MY_CONN");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString("You specified a connection (CONNECTION_NAME) and therefore should not specify"));
    }

    @Test
    void testNoDialect() {
        this.rawProperties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("You have to specify the SQL dialect "));
    }

    @Test
    void testInvalidDialect() {
        this.rawProperties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        this.rawProperties.put(SCHEMA_NAME_PROPERTY, "MY_SCHEMA");
        this.rawProperties.put(SQL_DIALECT_PROPERTY, "INVALID_DIALECT");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("SQL Dialect \"INVALID_DIALECT\" is not supported."));
    }

    @Test
    void testInvalidDebugAddress1() {
        getMinimumMandatory();
        this.rawProperties.put(DEBUG_ADDRESS_PROPERTY, "bla");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("You specified an invalid hostname and port"));
    }

    @Test
    void testInvalidDebugAddress2() {
        getMinimumMandatory();
        this.rawProperties.put(DEBUG_ADDRESS_PROPERTY, "bla:no-number");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("You specified an invalid hostname and port"));
    }

    @Test
    void testInvalidDebugAddress3() {
        getMinimumMandatory();
        this.rawProperties.put(DEBUG_ADDRESS_PROPERTY, "bla:123:456");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("You specified an invalid hostname and port"));
    }

    @Test
    void testValidDebugAddress() throws PropertyValidationException {
        getMinimumMandatory();
        this.rawProperties.put(DEBUG_ADDRESS_PROPERTY, "bla:123");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void testSchemaAndCatalogOptional() throws PropertyValidationException {
        this.rawProperties.put(SQL_DIALECT_PROPERTY, "GENERIC");
        this.rawProperties.put(CONNECTION_NAME_PROPERTY, "MY_CONN");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void checkValidBoolOption1() throws PropertyValidationException {
        getMinimumMandatory();
        this.rawProperties.put(IS_LOCAL_PROPERTY, "TrUe");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void checkValidBoolOption2() throws PropertyValidationException {
        getMinimumMandatory();
        this.rawProperties.put(IS_LOCAL_PROPERTY, "FalSe");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
    }

    @Test
    void checkInvalidBoolOption1() {
        getMinimumMandatory();
        this.rawProperties.put(IS_LOCAL_PROPERTY, "asdasd");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString(
                "The value 'asdasd' for the property IS_LOCAL is invalid. It has to be either 'true' or 'false' (case "
                        + "insensitive)"));
    }

    @Test
    void testInvalidExceptionHandling() {
        getMinimumMandatory();
        this.rawProperties.put(EXCEPTION_HANDLING_PROPERTY, "IGNORE_ALL");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("You specified an invalid exception mode (IGNORE_ALL)"));
    }

    @Test
    void getIgnoreErrors() {
        final Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put("IGNORE_ERRORS", "ERrror_foo, error_bar    ,  another_error, уккщк");
        final List<String> expectedErrorList = new ArrayList<>();
        final AdapterProperties adapterProperties = new AdapterProperties(rawProperties);
        final DummySqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        expectedErrorList.add("ERRROR_FOO");
        expectedErrorList.add("ERROR_BAR");
        expectedErrorList.add("ANOTHER_ERROR");
        expectedErrorList.add("УККЩК");
        assertEquals(expectedErrorList, sqlDialect.getIgnoredErrors());
    }

    @Test
    void testExasolSpecificPropertyImport() {
        getMinimumMandatory();
        this.rawProperties.put(EXASOL_IMPORT_PROPERTY, "EXASOL_IMPORT_PROPERTY");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("Do not use properties " + EXASOL_IMPORT_PROPERTY + " and "
                + EXASOL_CONNECTION_STRING_PROPERTY + " with GENERIC dialect"));
    }

    @Test
    void testExasolSpecificPropertyConnectionString() {
        getMinimumMandatory();
        this.rawProperties.put(EXASOL_CONNECTION_STRING_PROPERTY, "EXASOL_CONNECTION_STRING_PROPERTY");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString("Do not use properties " + EXASOL_IMPORT_PROPERTY + " and "
                + EXASOL_CONNECTION_STRING_PROPERTY + " with GENERIC dialect"));
    }

    @Test
    void testOracleSpecificPropertyImport() {
        getMinimumMandatory();
        this.rawProperties.put(ORACLE_IMPORT_PROPERTY, "ORACLE_IMPORT_PROPERTY");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString(
                        "Do not use properties " + ORACLE_IMPORT_PROPERTY + ", " + ORACLE_CONNECTION_NAME_PROPERTY
                                + " and " + ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY + " with GENERIC dialect"));
    }

    @Test
    void testOracleSpecificPropertyConnectionString() {
        getMinimumMandatory();
        this.rawProperties.put(ORACLE_CONNECTION_NAME_PROPERTY, "ORACLE_CONNECTION_NAME_PROPERTY");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString(
                        "Do not use properties " + ORACLE_IMPORT_PROPERTY + ", " + ORACLE_CONNECTION_NAME_PROPERTY
                                + " and " + ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY + " with GENERIC dialect"));
    }

    @Test
    void testOracleSpecificPropertyCastNumberToDecimal() {
        getMinimumMandatory();
        this.rawProperties.put(ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY, "ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString(
                        "Do not use properties " + ORACLE_IMPORT_PROPERTY + ", " + ORACLE_CONNECTION_NAME_PROPERTY
                                + " and " + ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY + " with GENERIC dialect"));
    }

    @Test
    void testPostgreSqlSpecificPropertyCastNumberToDecimal() {
        getMinimumMandatory();
        this.rawProperties.put(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY, "ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString(
                "Do not use property " + POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY + " with GENERIC dialect"));
    }
}