package com.exasol.adapter.dialects;

import static com.exasol.adapter.AdapterProperties.*;
import static com.exasol.adapter.dialects.exasol.ExasolProperties.EXASOL_CONNECTION_STRING_PROPERTY;
import static com.exasol.adapter.dialects.exasol.ExasolProperties.EXASOL_IMPORT_PROPERTY;
import static com.exasol.adapter.dialects.oracle.OracleProperties.*;
import static com.exasol.adapter.dialects.postgresql.PostgreSQLSqlDialect.POSTGRESQL_IDENTIFIER_MAPPING_PROPERTY;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.exasol.adapter.AdapterProperties;
import com.exasol.adapter.dialects.dummy.DummySqlDialect;
import com.exasol.logging.CapturingLogHandler;

class AbstractSqlDialectTest {
    private Map<String, String> rawProperties;
    private final CapturingLogHandler capturingLogHandler = new CapturingLogHandler();

    @BeforeEach
    void beforeEach() {
        Logger.getLogger("com.exasol").addHandler(this.capturingLogHandler);
        this.capturingLogHandler.reset();
        this.rawProperties = new HashMap<>();
    }

    @AfterEach
    void afterEach() {
        Logger.getLogger("com.exasol").removeHandler(this.capturingLogHandler);
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
    void testValidatePropertiesWithWherePortIsString() throws PropertyValidationException {
        this.rawProperties.put(DEBUG_ADDRESS_PROPERTY, "host:port_should_be_a_number");
        assertWarningIssued("Illegal debug output port");
    }

    private void assertWarningIssued(final String expectedMessagePart) throws PropertyValidationException {
        getMinimumMandatory();
        final AdapterProperties adapterProperties = new AdapterProperties(this.rawProperties);
        final SqlDialect sqlDialect = new DummySqlDialect(null, adapterProperties);
        sqlDialect.validateProperties();
        assertThat(this.capturingLogHandler.getCapturedData(), containsString(expectedMessagePart));
    }

    @Test
    void testValidatePropertiesWithWherePortTooLow() throws PropertyValidationException {
        this.rawProperties.put(DEBUG_ADDRESS_PROPERTY, "host:0");
        assertWarningIssued("Debug output port 0 is out of range");
    }

    @Test
    void testValidatePropertiesWithWherePortTooHigh() throws PropertyValidationException {
        this.rawProperties.put(DEBUG_ADDRESS_PROPERTY, "host:65536");
        assertWarningIssued("Debug output port 65536 is out of range");
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
        assertThat(sqlDialect.getStringLiteral(original), equalTo(literal));
    }

    @Test
    void testGetStringLiteralWithNull() {
        final SqlDialect sqlDialect = new DummySqlDialect(null, AdapterProperties.emptyProperties());
        assertThat(sqlDialect.getStringLiteral(null), equalTo("NULL"));
    }
}
