package com.exasol.adapter.jdbc;

import com.exasol.adapter.AdapterException;
import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JdbcAdapterPropertiesTest {
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    private static Map<String, String> getMinimumMandatory() {
        Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_SQL_DIALECT, "GENERIC");
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_NAME, "MY_CONN");
        return properties;
    }
    
    @Test
    public void testNoCredentials() throws AdapterException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_SQL_DIALECT, "GENERIC");
        properties.put(JdbcAdapterProperties.PROP_SCHEMA_NAME, "MY_SCHEMA");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("You did not specify a connection (CONNECTION_NAME) and therefore have to specify");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }

    @Test
    public void testUserNamePasswordOptional() throws AdapterException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_SQL_DIALECT, "GENERIC");
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_STRING, "MY_CONN");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }
    
    @Test
    public void testRedundantCredentials() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_STRING, "MY_CONN");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("You specified a connection (CONNECTION_NAME) and therefore may not specify ");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
        
        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_USERNAME, "MY_USER");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("You specified a connection (CONNECTION_NAME) and therefore may not specify ");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
        
        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_PASSWORD, "MY_PASSWORD");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("You specified a connection (CONNECTION_NAME) and therefore may not specify ");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }

    @Test
    public void testNoDialect() throws AdapterException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_NAME, "MY_CONN");
        properties.put(JdbcAdapterProperties.PROP_SCHEMA_NAME, "MY_SCHEMA");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("You have to specify the SQL dialect");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }

    @Test
    public void testInvalidDialect() throws AdapterException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_NAME, "MY_CONN");
        properties.put(JdbcAdapterProperties.PROP_SCHEMA_NAME, "MY_SCHEMA");
        properties.put(JdbcAdapterProperties.PROP_SQL_DIALECT, "INVALID_DIALECT");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("SQL Dialect not supported");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }

    @Test
    public void testInvalidDebugAddress1() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_DEBUG_ADDRESS, "bla");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("You specified an invalid hostname and port");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }

    @Test
    public void testInvalidDebugAddress2() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_DEBUG_ADDRESS, "bla:no-number");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("You specified an invalid hostname and port");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }

    @Test
    public void testInvalidDebugAddress3() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_DEBUG_ADDRESS, "bla:123:456");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("You specified an invalid hostname and port");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }

    @Test
    public void testValidDebugAddress() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_DEBUG_ADDRESS, "bla:123");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }
    
    @Test
    public void testSchemaAndCatalogOptional() throws AdapterException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JdbcAdapterProperties.PROP_SQL_DIALECT, "GENERIC");
        properties.put(JdbcAdapterProperties.PROP_CONNECTION_NAME, "MY_CONN");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }
    
    @Test
    public void checkValidBoolOptions() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IS_LOCAL, "TrUe");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
        
        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IS_LOCAL, "FalSe");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
        
        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IMPORT_FROM_EXA, "TrUe");
        properties.put(JdbcAdapterProperties.PROP_EXA_CONNECTION_STRING, "localhost:5555");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
        
        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IMPORT_FROM_EXA, "FalSe");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }
    
    @Test
    public void checkInvalidBoolOption() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IS_LOCAL, "asdasd");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("The value 'asdasd' for the property IS_LOCAL is invalid. It has to be either 'true' or 'false' (case insensitive)");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
        
        properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IMPORT_FROM_EXA, "asdasd");
        properties.put(JdbcAdapterProperties.PROP_EXA_CONNECTION_STRING, "localhost:5555");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("The value 'asdasd' for the property IMPORT_FROM_EXA is invalid. It has to be either 'true' or 'false' (case insensitive)");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }

    @Test
    public void testInconsistentExaProperties() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_EXA_CONNECTION_STRING, "localhost:5555");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("You defined the property EXA_CONNECTION_STRING without setting IMPORT_FROM_EXA");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }

    @Test
    public void testInvalidExaProperties2() throws AdapterException {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_IMPORT_FROM_EXA, "True");
        thrown.expect(AdapterException.class);
        thrown.expectMessage("You defined the property IMPORT_FROM_EXA, please also define EXA_CONNECTION_STRING");
        JdbcAdapterProperties.checkPropertyConsistency(properties, JdbcAdapter.supportedDialects);
    }
    
    @Test
    public void testGetTableFilters() {
        Map<String, String> properties = getMinimumMandatory();
        properties.put(JdbcAdapterProperties.PROP_TABLES, "T1, T2,T3,t4");
        List<String> expectedTables = ImmutableList.of("T1", "T2", "T3", "t4");
        assertEquals(expectedTables, JdbcAdapterProperties.getTableFilter(properties));
    }
    
    @Test
    public void testGetNewSchemaMetadata() {
        Map<String, String> oldSchemaProperties = new HashMap<String, String>();
        oldSchemaProperties.put("EXISTING_PROP_1", "Old Value 1");
        oldSchemaProperties.put("EXISTING_PROP_2", "Old Value 2");
        
        Map<String, String> changedProperties = new HashMap<String, String>();
        changedProperties.put("EXISTING_PROP_1", "New Value");
        changedProperties.put("EXISTING_PROP_2", null);
        changedProperties.put("NEW_PROP", "VAL2");
        changedProperties.put("DELETED_PROP_NON_EXISTING", null);
        
        Map<String, String> expectedChangedProperties = new HashMap<String, String>();
        expectedChangedProperties.put("EXISTING_PROP_1", "New Value");
        expectedChangedProperties.put("NEW_PROP", "VAL2");
        
        assertEquals(expectedChangedProperties, JdbcAdapterProperties.getNewProperties(oldSchemaProperties, changedProperties));
    }

}
