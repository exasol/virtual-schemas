package com.exasol.adapter.jdbc;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.exasol.adapter.AdapterException;
import com.google.common.collect.ImmutableList;

public class JdbcAdapterPropertiesTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static Map<String, String> getMinimumMandatory() {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_SQL_DIALECT, "GENERIC");
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_NAME, "MY_CONN");
        return properties;
    }

    @Test
    public void testNoCredentials() throws AdapterException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_SQL_DIALECT, "GENERIC");
        properties.put(JdbcAdapterProperties.PROP_SCHEMA_NAME, "MY_SCHEMA");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("You did not specify a connection (CONNECTION_NAME) and therefore have to specify");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testUserNamePasswordOptional() throws AdapterException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_SQL_DIALECT, "GENERIC");
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_STRING, "MY_CONN");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testRedundantCredentials() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_STRING, "MY_CONN");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("You specified a connection (CONNECTION_NAME) and therefore may not specify ");
        JdbcAdapterProperties.checkPropertyConsistency(properties);

        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_USERNAME, "MY_USER");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("You specified a connection (CONNECTION_NAME) and therefore may not specify ");
        JdbcAdapterProperties.checkPropertyConsistency(properties);

        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_PASSWORD, "MY_PASSWORD");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("You specified a connection (CONNECTION_NAME) and therefore may not specify ");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testNoDialect() throws AdapterException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_NAME, "MY_CONN");
        properties.put(JdbcAdapterProperties.PROP_SCHEMA_NAME, "MY_SCHEMA");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("You have to specify the SQL dialect");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testInvalidDialect() throws AdapterException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_NAME, "MY_CONN");
        properties.put(JdbcAdapterProperties.PROP_SCHEMA_NAME, "MY_SCHEMA");
        properties.put(JdbcAdapterProperties.PROP_SQL_DIALECT, "INVALID_DIALECT");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("SQL Dialect \"INVALID_DIALECT\" is not supported.");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testInvalidDebugAddress1() throws AdapterException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_DEBUG_ADDRESS, "bla");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("You specified an invalid hostname and port");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testInvalidDebugAddress2() throws AdapterException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_DEBUG_ADDRESS, "bla:no-number");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("You specified an invalid hostname and port");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testInvalidDebugAddress3() throws AdapterException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_DEBUG_ADDRESS, "bla:123:456");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("You specified an invalid hostname and port");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testValidDebugAddress() throws AdapterException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_DEBUG_ADDRESS, "bla:123");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testSchemaAndCatalogOptional() throws AdapterException {
        final Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_SQL_DIALECT, "GENERIC");
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_NAME, "MY_CONN");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void checkValidBoolOptions() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IS_LOCAL, "TrUe");
        JdbcAdapterProperties.checkPropertyConsistency(properties);

        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IS_LOCAL, "FalSe");
        JdbcAdapterProperties.checkPropertyConsistency(properties);

        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IMPORT_FROM_EXA, "TrUe");
        properties.put(JdbcAdapterProperties.PROP_EXA_CONNECTION_STRING, "localhost:5555");
        JdbcAdapterProperties.checkPropertyConsistency(properties);

        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IMPORT_FROM_EXA, "FalSe");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void checkInvalidBoolOption() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IS_LOCAL, "asdasd");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage(
                "The value 'asdasd' for the property IS_LOCAL is invalid. It has to be either 'true' or 'false' (case insensitive)");
        JdbcAdapterProperties.checkPropertyConsistency(properties);

        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IMPORT_FROM_EXA, "asdasd");
        properties.put(JdbcAdapterProperties.PROP_EXA_CONNECTION_STRING, "localhost:5555");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage(
                "The value 'asdasd' for the property IMPORT_FROM_EXA is invalid. It has to be either 'true' or 'false' (case insensitive)");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testInconsistentExaProperties() throws AdapterException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_EXA_CONNECTION_STRING, "localhost:5555");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("You defined the property EXA_CONNECTION_STRING without setting IMPORT_FROM_EXA");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testInvalidExaProperties2() throws AdapterException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IMPORT_FROM_EXA, "True");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("You defined the property IMPORT_FROM_EXA, please also define EXA_CONNECTION_STRING");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testGetTableFilters() {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_TABLES, "T1, T2,T3,t4");
        final List<String> expectedTables = ImmutableList.of("T1", "T2", "T3", "t4");
        assertEquals(expectedTables, JdbcAdapterProperties.getTableFilter(properties));
    }

    @Test
    public void testGetNewSchemaMetadata() {
        final Map<String, String> oldSchemaProperties = new HashMap<String, String>();
        oldSchemaProperties.put("EXISTING_PROP_1", "Old Value 1");
        oldSchemaProperties.put("EXISTING_PROP_2", "Old Value 2");

        final Map<String, String> changedProperties = new HashMap<String, String>();
        changedProperties.put("EXISTING_PROP_1", "New Value");
        changedProperties.put("EXISTING_PROP_2", null);
        changedProperties.put("NEW_PROP", "VAL2");
        changedProperties.put("DELETED_PROP_NON_EXISTING", null);

        final Map<String, String> expectedChangedProperties = new HashMap<String, String>();
        expectedChangedProperties.put("EXISTING_PROP_1", "New Value");
        expectedChangedProperties.put("NEW_PROP", "VAL2");

        assertEquals(expectedChangedProperties,
                JdbcAdapterProperties.getNewProperties(oldSchemaProperties, changedProperties));
    }

    @Test
    public void testNullInExceptionHandling() throws AdapterException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_EXCEPTION_HANDLING, null);
        assertEquals(JdbcAdapterProperties.ExceptionHandlingMode.NONE,
                JdbcAdapterProperties.getExceptionHandlingMode(properties));
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testEmptyExceptionHandling() throws AdapterException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_EXCEPTION_HANDLING, "");
        assertEquals(JdbcAdapterProperties.ExceptionHandlingMode.NONE,
                JdbcAdapterProperties.getExceptionHandlingMode(properties));
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testInvalidExceptionHandling() throws AdapterException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_EXCEPTION_HANDLING, "IGNORE_ALL");
        this.thrown.expect(AdapterException.class);
        this.thrown.expectMessage("You specified an invalid exception mode (IGNORE_ALL)");
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void testNoneAsExceptionValue() throws AdapterException {
        final Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_EXCEPTION_HANDLING, "NONE");
        assertEquals(JdbcAdapterProperties.ExceptionHandlingMode.NONE,
                JdbcAdapterProperties.getExceptionHandlingMode(properties));
        JdbcAdapterProperties.checkPropertyConsistency(properties);
    }

    @Test
    public void getIgnoreErrorList() {
        Map<String, String> properties = new HashMap<>();
        properties.put("IGNORE_ERRORS", "ERrror_foo, error_bar    ,  another_error, уккщк");
        List<String> expectedErrorList = new ArrayList<>();
        expectedErrorList.add("ERRROR_FOO");
        expectedErrorList.add("ERROR_BAR");
        expectedErrorList.add("ANOTHER_ERROR");
        expectedErrorList.add("УККЩК");
        assertEquals(expectedErrorList, JdbcAdapterProperties.getIgnoreErrorList(properties));
    }
}
