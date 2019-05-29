package com.exasol.adapter.dialects;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.dialects.exasol.ExasolProperties.EXASOL_CONNECTION_STRING_PROPERTY;
import static com.exasol.adapter.dialects.exasol.ExasolProperties.EXASOL_IMPORT_PROPERTY;
import static com.exasol.adapter.dialects.oracle.OracleProperties.*;
import static com.exasol.adapter.dialects.postgresql.PostgreSQLSqlDialect.POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.*;

import org.junit.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.exasol.adapter.AdapterProperties;
import org.junit.jupiter.params.*;
import org.junit.jupiter.params.provider.*;

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
        assertThat(exception.getMessage(), containsString(
                "You did not specify a connection using the property CONNECTION_NAME and therefore have to specify the property "
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
        assertThat(exception.getMessage(), containsString(
                "You specified a connection using the property CONNECTION_NAME and therefore should not specify"));
    }

    @Test
    void testRedundantCredentialsPassword() {
        getMinimumMandatory();
        this.rawProperties.put(PASSWORD_PROPERTY, "MY_PASSWORD");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString(
                "You specified a connection using the property CONNECTION_NAME and therefore should not specify"));
    }

    @Test
    void testRedundantCredentialsConnectionString() {
        getMinimumMandatory();
        this.rawProperties.put(CONNECTION_STRING_PROPERTY, "MY_CONN");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString(
                "You specified a connection using the property CONNECTION_NAME and therefore should not specify"));
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
    void testInvalidExceptionHandling() {
        getMinimumMandatory();
        this.rawProperties.put(EXCEPTION_HANDLING_PROPERTY, "IGNORE_ALL");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString(
                "Invalid value 'IGNORE_ALL' for property EXCEPTION_HANDLING. Choose one of: IGNORE_INVALID_VIEWS, NONE"));
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
        assertThat(exception.getMessage(),
                containsString("The dialect GENERIC does not support IMPORT_FROM_EXA property."));
    }

    @Test
    void testExasolSpecificPropertyConnectionString() {
        getMinimumMandatory();
        this.rawProperties.put(EXASOL_CONNECTION_STRING_PROPERTY, "EXASOL_CONNECTION_STRING_PROPERTY");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString("The dialect GENERIC does not support EXA_CONNECTION_STRING property."));
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
                containsString("The dialect GENERIC does not support IMPORT_FROM_ORA property."));
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
                containsString("The dialect GENERIC does not support ORA_CONNECTION_NAME property."));
    }

    @Test
    void testOracleSpecificPropertyCastNumberToDecimal() {
        getMinimumMandatory();
        this.rawProperties.put(ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY, "ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(), containsString(
                "The dialect GENERIC does not support ORACLE_CAST_NUMBER_TO_DECIMAL_WITH_PRECISION_AND_SCALE property."));
    }

    @Test
    void testPostgreSqlSpecificPropertyCastNumberToDecimal() {
        getMinimumMandatory();
        this.rawProperties.put(POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY, "ORACLE_CAST_NUMBER_TO_DECIMAL_PROPERTY");
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        final PropertyValidationException exception = assertThrows(PropertyValidationException.class,
                sqlDialect::validateProperties);
        assertThat(exception.getMessage(),
                containsString("The dialect GENERIC does not support POSTGRESQL_IDENTIFIER_MAPPING property."));
    }

    @ValueSource(strings = { "ab:\'ab\'", "a'b:'a''b'", "a''b:'a''''b'", "'ab':'''ab'''" })
    @ParameterizedTest
    void testGetLiteralString(final String definition) {
        final int colonPosition = definition.indexOf(':');
        final String original = definition.substring(0, colonPosition);
        final String literal = definition.substring(colonPosition + 1);
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        Assert.assertThat(sqlDialect.getStringLiteral(original), equalTo(literal));
    }
}