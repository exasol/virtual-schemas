package com.exasol.adapter.jdbc;

import com.exasol.ExaConnectionAccessException;
import com.exasol.ExaConnectionInformation;
import com.exasol.ExaMetadata;
import com.exasol.adapter.AdapterException;
import com.exasol.adapter.dialects.SqlDialect;
import com.exasol.adapter.dialects.SqlDialectContext;
import com.exasol.adapter.dialects.SqlDialects;

import java.util.*;

/**
 * Class to expose a nice interface to properties. Casts to the correct data types, checks for valid property values and consistency.
 */
public class JdbcAdapterProperties {

    // One of the following needs to be set
    static final String PROP_CATALOG_NAME = "CATALOG_NAME";
    static final String PROP_SCHEMA_NAME = "SCHEMA_NAME";
    static final String PROP_CONNECTION_NAME = "CONNECTION_NAME";
    static final String PROP_CONNECTION_STRING = "CONNECTION_STRING";
    static final String PROP_USERNAME = "USERNAME";
    static final String PROP_PASSWORD = "PASSWORD";

    // Optional Parameters
    static final String PROP_TABLES = "TABLE_FILTER";
    static final String PROP_DEBUG_ADDRESS = "DEBUG_ADDRESS";
    static final String PROP_IS_LOCAL = "IS_LOCAL";
    static final String PROP_SQL_DIALECT = "SQL_DIALECT";
    static final String PROP_IMPORT_FROM_EXA = "IMPORT_FROM_EXA";
    static final String PROP_EXA_CONNECTION_STRING = "EXA_CONNECTION_STRING";
    static final String PROP_EXCLUDED_CAPABILITIES = "EXCLUDED_CAPABILITIES";
    static final String PROP_EXCEPTION_HANDLING = "EXCEPTION_HANDLING";

    // Specifies different exception handling strategies
    public enum ExceptionHandlingMode {
        IGNORE_INVALID_VIEWS,
        NONE
    }

    private static String getProperty(Map<String, String> properties, String name, String defaultValue) {
        if (properties.containsKey(name)) {
            return properties.get(name);
        } else {
            return defaultValue;
        }
    }
    
    public static String getCatalog(Map<String, String> properties) {
        return getProperty(properties, PROP_CATALOG_NAME, "");
    }
    
    public static String getSchema(Map<String, String> properties) {
        return getProperty(properties, PROP_SCHEMA_NAME, "");
    }

    public static boolean userSpecifiedConnection(Map<String, String> properties) {
        String connName = getProperty(properties, PROP_CONNECTION_NAME, "");
        return (connName != null && !connName.isEmpty());
    }

    public static String getConnectionName(Map<String, String> properties) {
        String connName = getProperty(properties, PROP_CONNECTION_NAME, "");
        assert(connName != null && !connName.isEmpty());
        return connName;
    }

    /**
     * Returns the credentials for the remote system. These are either directly specified
     * in the properties or obtained from a connection (requires privilege to access the connection
     * .
     */
    public static ExaConnectionInformation getConnectionInformation(Map<String, String> properties, ExaMetadata exaMeta) {
        String connName = getProperty(properties, PROP_CONNECTION_NAME, "");
        if (connName != null && !connName.isEmpty()) {
            try {
                ExaConnectionInformation connInfo = exaMeta.getConnection(connName);
                return connInfo;
            } catch (ExaConnectionAccessException e) {
                throw new RuntimeException("Could not access the connection information of connection " + connName + ". Error: " + e.toString());
            }
        } else {
            String connectionString = properties.get(PROP_CONNECTION_STRING);
            String user = properties.get(PROP_USERNAME);
            String password = properties.get(PROP_PASSWORD);
            return new ExaConnectionInformationJdbc(connectionString, user, password);
        }
    }
    
    public static void checkPropertyConsistency(Map<String, String> properties, SqlDialects supportedDialects) throws AdapterException {
        validatePropertyValues(properties);
        
        checkMandatoryProperties(properties, supportedDialects);
        
        if (isImportFromExa(properties)) {
            if (getExaConnectionString(properties).isEmpty()) {
                throw new InvalidPropertyException("You defined the property " + PROP_IMPORT_FROM_EXA + ", please also define " + PROP_EXA_CONNECTION_STRING);
            }
        } else {
            if (!getExaConnectionString(properties).isEmpty()) {
                throw new InvalidPropertyException("You defined the property " + PROP_EXA_CONNECTION_STRING + " without setting " + PROP_IMPORT_FROM_EXA + " to 'TRUE'. This is not allowed");
            }
        }
    }

    private static void validatePropertyValues(Map<String, String> properties) throws AdapterException {
        validateBooleanProperty(properties, PROP_IS_LOCAL);
        validateBooleanProperty(properties, PROP_IMPORT_FROM_EXA);
        if (properties.containsKey(PROP_DEBUG_ADDRESS)) {
            validateDebugOutputAddress(properties.get(PROP_DEBUG_ADDRESS));
        }
        if (properties.containsKey(PROP_EXCEPTION_HANDLING)) {
            validateExceptionHandling(properties.get(PROP_EXCEPTION_HANDLING));
        }
    }
    
    private static void validateBooleanProperty(Map<String, String> properties, String property) throws AdapterException {
        if (properties.containsKey(property)) {
            if (!properties.get(property).toUpperCase().matches("^TRUE$|^FALSE$")) {
                throw new InvalidPropertyException("The value '" + properties.get(property) + "' for the property " + property + " is invalid. It has to be either 'true' or 'false' (case insensitive).");
            }
        }
    }

    private static void validateDebugOutputAddress(String debugAddress) throws AdapterException {
        if (!debugAddress.isEmpty()) {
            String error = "You specified an invalid hostname and port for the udf debug service (" + PROP_DEBUG_ADDRESS + "). Please provide a valid value, e.g. 'hostname:3000'";
            try {
                String debugHost = debugAddress.split(":")[0];
                int debugPort = Integer.parseInt(debugAddress.split(":")[1]);
            } catch (Exception ex) {
                throw new AdapterException(error);
            }
            if (debugAddress.split(":").length != 2) {
                throw new AdapterException(error);
            }
        }
    }

    private static void validateExceptionHandling(String exceptionHandling) throws AdapterException {
        if (!(exceptionHandling == null || exceptionHandling.isEmpty())) {
            for (ExceptionHandlingMode mode : ExceptionHandlingMode.values()) {
                if (mode.name().equals(exceptionHandling)) {
                    return;
                }
            }
            String error = "You specified an invalid exception mode (" + exceptionHandling + ").";
            throw new AdapterException(error);
        }
    }

    private static void checkMandatoryProperties(Map<String, String> properties, SqlDialects supportedDialects) throws AdapterException {
        if (!properties.containsKey(PROP_SQL_DIALECT)) {
            throw new InvalidPropertyException("You have to specify the SQL dialect (" + PROP_SQL_DIALECT + "). Available dialects: " + supportedDialects.getDialectsString());
        }
        if (!supportedDialects.isSupported(properties.get(PROP_SQL_DIALECT))) {
            throw new InvalidPropertyException("SQL Dialect not supported: " + properties.get(PROP_SQL_DIALECT) + ". Available dialects: " + supportedDialects.getDialectsString());
        }
        if (properties.containsKey(PROP_CONNECTION_NAME)) {
            if (properties.containsKey(PROP_CONNECTION_STRING) || properties.containsKey(PROP_USERNAME) || properties.containsKey(PROP_PASSWORD) ) {
                throw new InvalidPropertyException("You specified a connection (" + PROP_CONNECTION_NAME + ") and therefore may not specify the properties " + PROP_CONNECTION_STRING + ", " + PROP_USERNAME + " and " + PROP_PASSWORD);
            }
        } else {
            if (!properties.containsKey(PROP_CONNECTION_STRING)) {
                throw new InvalidPropertyException("You did not specify a connection (" + PROP_CONNECTION_NAME + ") and therefore have to specify the property " + PROP_CONNECTION_STRING);
            }
        }
    }
    
    public static boolean isImportFromExa(Map<String, String> properties) {
        return getProperty(properties, PROP_IMPORT_FROM_EXA, "").toUpperCase().equals("TRUE");
    }

    public static List<String> getTableFilter(Map<String, String> properties) {
        String tableNames = getProperty(properties, PROP_TABLES, "");
        if (!tableNames.isEmpty()) {
            List<String> tables = Arrays.asList(tableNames.split(","));
            for (int i=0; i<tables.size();++i) {
                tables.set(i, tables.get(i).trim());
            }
            return tables;
        } else {
            return new ArrayList<>();
        }
    }

    public static String getExcludedCapabilities(Map<String, String> properties) {
        return getProperty(properties, PROP_EXCLUDED_CAPABILITIES, "");
    }

    public static String getDebugAddress(Map<String, String> properties) {
        return getProperty(properties, PROP_DEBUG_ADDRESS, "");
    }

    public static boolean isLocal(Map<String, String> properties) {
        return getProperty(properties, PROP_IS_LOCAL, "").toUpperCase().equals("TRUE");
    }

    public static String getSqlDialectName(Map<String, String> properties, SqlDialects supportedDialects) {
        return getProperty(properties, PROP_SQL_DIALECT, "");
    }

    public static SqlDialect getSqlDialect(Map<String, String> properties, SqlDialects supportedDialects, SqlDialectContext dialectContext) throws AdapterException {
        String dialectName = getProperty(properties, PROP_SQL_DIALECT, "");
        SqlDialect dialect = supportedDialects.getDialectByName(dialectName, dialectContext);
        if (dialect == null) {
            throw new InvalidPropertyException("SQL Dialect not supported: " + dialectName + " - all dialects: " + supportedDialects.getDialectsString());
        }
        return dialect;
    }

    public static String getExaConnectionString(Map<String, String> properties) {
        return getProperty(properties, PROP_EXA_CONNECTION_STRING, "");
    }

    public static ExceptionHandlingMode getExceptionHandlingMode(Map<String, String> properties) {
        String propertyValue = getProperty(properties, PROP_EXCEPTION_HANDLING, "");
        if (propertyValue == null || propertyValue.isEmpty()) {
            return ExceptionHandlingMode.NONE;
        }
        for (ExceptionHandlingMode mode : ExceptionHandlingMode.values()) {
            if (mode.name().equals(propertyValue)) {
                return mode;
            }
        }
        return ExceptionHandlingMode.NONE;
    }

    public static boolean isRefreshNeeded(Map<String, String> newProperties) {
        return newProperties.containsKey(PROP_CONNECTION_STRING)
                || newProperties.containsKey(PROP_CONNECTION_NAME)
                || newProperties.containsKey(PROP_USERNAME)
                || newProperties.containsKey(PROP_PASSWORD)
                || newProperties.containsKey(PROP_SCHEMA_NAME)
                || newProperties.containsKey(PROP_CATALOG_NAME)
                || newProperties.containsKey(PROP_TABLES);
    }
    
    public static class ExaConnectionInformationJdbc implements ExaConnectionInformation {
        
        private String address;
        private String user;        // can be null
        private String password;    // can be null
        
        public ExaConnectionInformationJdbc(String address, String user, String password) {
            this.address = address;
            this.user = user;
            this.password = password;
        }

        @Override
        public ConnectionType getType() {
            return ConnectionType.PASSWORD;
        }

        @Override
        public String getAddress() {
            return this.address;
        }

        @Override
        public String getUser() {
            return this.user;
        }

        @Override
        public String getPassword() {
            return this.password;
        }
    }

    /**
     * Returns the properties as they would be after successfully applying the changes to the existing (old) set of properties.
     */
    public static Map<String, String> getNewProperties (
            Map<String, String> oldProperties, Map<String, String> changedProperties) {
        Map<String, String> newCompleteProperties = new HashMap<>(oldProperties);
        for (Map.Entry<String, String> changedProperty : changedProperties.entrySet()) {
            if (changedProperty.getValue() == null) {
                // Null values represent properties which are deleted by the user (might also have never existed actually)
                newCompleteProperties.remove(changedProperty.getKey());
            } else {
                newCompleteProperties.put(changedProperty.getKey(), changedProperty.getValue());
            }
        }
        return newCompleteProperties;
    }

}
